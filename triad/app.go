package triad

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"creditnet/core"
)

const (
	MaxNovPerLink  = 3
	MaxAmount      = uint64(1<<63 - 1)
	NovateInterval = 5 * time.Minute
)

type Novation struct {
	Amount uint64 `json:"amount"`; Role string `json:"role"`
	InPeer string `json:"in_peer,omitempty"`; OutPeer string `json:"out_peer,omitempty"`
	Queue         []string `json:"queue,omitempty"`
}

func (n *Novation) pendingPeer() string {
	if core.QueueHead(n.Queue) == "rollback" { return n.InPeer }; return n.OutPeer
}

type commitMsg struct {
	NovationID string `json:"novation_id"`; Debtor string `json:"debtor"`
	Intermediary string `json:"intermediary"`; Creditor string `json:"creditor"`; Amount uint64 `json:"amount"`
}
type finalizeMsg struct { NovationID string `json:"novation_id"` }

type App struct {
	Novations map[string]*Novation `json:"novations,omitempty"`

	id            string
	links         map[string]*core.Link
	sendFn        core.SendFunc
	scheduleFn    core.ScheduleFunc
	bwFn          core.BandwidthFunc
	pbFn          core.PendingBalanceFunc
	receiptFn     core.ReceiptFunc
	module        string
	kicks         []string
}

func New(nodeID string, links map[string]*core.Link, state json.RawMessage) core.Module {
	a := &App{Novations: map[string]*Novation{}, id: nodeID, links: links}
	if state != nil { json.Unmarshal(state, a) }
	if a.Novations == nil { a.Novations = map[string]*Novation{} }
	return a
}

func (a *App) SetCallbacks(module string, send core.SendFunc, deferSend core.DeferSendFunc, schedule core.ScheduleFunc, bw core.BandwidthFunc, pb core.PendingBalanceFunc, receipt core.ReceiptFunc, transfer core.TransferFunc) {
	a.module = module; a.sendFn = send; a.scheduleFn = schedule; a.bwFn = bw; a.pbFn = pb; a.receiptFn = receipt
}
func (a *App) DrainKicks() []string { k := a.kicks; a.kicks = nil; return k }

func (a *App) ScheduleTimers() {
	if a.scheduleFn != nil { a.scheduleFn(NovateInterval, a.novateAndReschedule) }
}
func (a *App) novateAndReschedule() {
	a.tryNovate()
	if a.scheduleFn != nil { a.scheduleFn(NovateInterval, a.novateAndReschedule) }
}

func (a *App) OnRemoveLink(peer string) {
	for id, nov := range a.Novations {
		if nov.InPeer == peer { nov.InPeer = "" }
		if nov.OutPeer == peer { nov.OutPeer = ""; nov.Queue = nil }
		if nov.InPeer == "" && nov.OutPeer == "" { delete(a.Novations, id) }
	}
}

func (a *App) novLinkCount(peer string) int {
	c := 0; for _, n := range a.Novations { if n.InPeer == peer || n.OutPeer == peer { c++ } }; return c
}

func (a *App) Reserved(peer string, side int) uint64 {
	var t uint64
	for _, n := range a.Novations {
		if side == core.IN && n.InPeer == peer { t += n.Amount }
		if side == core.OUT && n.OutPeer == peer { t += n.Amount }
	}
	return t
}

func (a *App) Status() json.RawMessage {
	return core.Enc(map[string]any{"novations": len(a.Novations)})
}

func (a *App) NextTx(peer string) *core.QueuedMsg {
	for id, nov := range a.Novations {
		if len(nov.Queue) > 0 && nov.pendingPeer() == peer {
			return a.buildMsg(id, nov)
		}
	}
	return nil
}

func (a *App) buildMsg(id string, nov *Novation) *core.QueuedMsg {
	switch core.QueueHead(nov.Queue) {
	case "commit":
		var d, i, c string
		switch nov.Role {
		case "intermediary": d, i, c = nov.OutPeer, a.id, nov.InPeer
		case "debtor":       d, i, c = a.id, nov.InPeer, nov.OutPeer
		case "creditor":     d, i, c = nov.InPeer, nov.OutPeer, a.id
		}
		return &core.QueuedMsg{Method: "commit", Payload: core.Enc(commitMsg{NovationID: id, Debtor: d, Intermediary: i, Creditor: c, Amount: nov.Amount})}
	case "finalize":
		return &core.QueuedMsg{Method: "finalize", Payload: core.Enc(finalizeMsg{NovationID: id})}
	case "rollback":
		return &core.QueuedMsg{Method: "rollback", Payload: core.Enc(map[string]string{"novation_id": id})}
	}; return nil
}

func (a *App) HandleRequest(peer string, ls *core.Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "commit":   return a.reqCommit(peer, ls, payload)
	case "finalize": return a.reqFinalize(peer, ls, payload, ts)
	case "rollback": return a.reqRollback(peer, payload)
	default:         return false, nil
	}
}

func (a *App) HandleResponse(peer string, ls *core.Link, item core.QueuedMsg, ok bool, ts int64) {
	switch item.Method {
	case "commit":   a.respCommit(peer, item.Payload, ok)
	case "finalize": a.respFinalize(peer, ls, item.Payload, ts)
	case "rollback": a.respRollback(item.Payload)
	}
}

func (a *App) reqCommit(peer string, ls *core.Link, raw json.RawMessage) (bool, error) {
	var m commitMsg; if json.Unmarshal(raw, &m) != nil { return false, fmt.Errorf("bad payload") }
	if m.Amount == 0 || m.Amount > MaxAmount { return false, nil }
	switch a.id {
	case m.Debtor:
		if peer != m.Intermediary { return false, nil }
		if a.pbFn(peer, core.IN) < int64(m.Amount) { return false, nil }
		if a.Novations[m.NovationID] != nil || a.links[m.Creditor] == nil { return false, nil }
		if a.novLinkCount(peer) >= MaxNovPerLink || a.novLinkCount(m.Creditor) >= MaxNovPerLink { return false, nil }
		a.Novations[m.NovationID] = &Novation{Amount: m.Amount, Role: "debtor", InPeer: peer, OutPeer: m.Creditor, Queue: []string{"commit"}}
		a.kicks = append(a.kicks, m.Creditor); return true, nil
	case m.Creditor:
		if peer != m.Debtor { return false, nil }
		if a.bwFn(peer, core.IN) < m.Amount { return false, nil }
		il := a.links[m.Intermediary]; if il == nil { return false, nil }
		if a.pbFn(m.Intermediary, core.OUT) < int64(m.Amount) { return false, nil }
		if a.Novations[m.NovationID] != nil { return false, nil }
		if a.novLinkCount(peer) >= MaxNovPerLink || a.novLinkCount(m.Intermediary) >= MaxNovPerLink { return false, nil }
		a.Novations[m.NovationID] = &Novation{Amount: m.Amount, Role: "creditor", InPeer: peer, OutPeer: m.Intermediary, Queue: []string{"commit"}}
		a.kicks = append(a.kicks, m.Intermediary); return true, nil
	case m.Intermediary:
		if peer != m.Creditor { return false, nil }
		nov := a.Novations[m.NovationID]; if nov == nil || nov.Role != "intermediary" { return false, nil }
		core.Enqueue(&nov.Queue, "finalize"); a.kicks = append(a.kicks, nov.OutPeer); return true, nil
	default: return false, nil
	}
}

func (a *App) respCommit(peer string, raw json.RawMessage, ok bool) {
	var m commitMsg; if json.Unmarshal(raw, &m) != nil { return }
	nov := a.Novations[m.NovationID]; if nov == nil { return }; core.Dequeue(&nov.Queue)
	if !ok { if nov.Role == "intermediary" { delete(a.Novations, m.NovationID); return }; core.Enqueue(&nov.Queue, "rollback"); a.kicks = append(a.kicks, nov.InPeer) }
}

func (a *App) reqFinalize(peer string, ls *core.Link, raw json.RawMessage, ts int64) (bool, error) {
	var m finalizeMsg; if json.Unmarshal(raw, &m) != nil { return false, fmt.Errorf("bad payload") }
	nov := a.Novations[m.NovationID]; if nov == nil { return false, fmt.Errorf("novation %s not found", m.NovationID) }
	if nov.InPeer != peer { return false, fmt.Errorf("novation %s: expected peer %s, got %s", m.NovationID, nov.InPeer, peer) }
	ls.AddBalance(int64(nov.Amount), ts); nov.InPeer = ""
	if nov.OutPeer != "" { core.Enqueue(&nov.Queue, "finalize"); a.kicks = append(a.kicks, nov.OutPeer) }
	if nov.InPeer == "" && nov.OutPeer == "" { delete(a.Novations, m.NovationID) }
	return true, nil
}

func (a *App) respFinalize(peer string, ls *core.Link, raw json.RawMessage, ts int64) {
	var m finalizeMsg; if json.Unmarshal(raw, &m) != nil { return }
	nov := a.Novations[m.NovationID]; if nov == nil { return }
	core.Dequeue(&nov.Queue)
	ls.AddBalance(-int64(nov.Amount), ts); nov.OutPeer = ""
	if nov.InPeer == "" { delete(a.Novations, m.NovationID) }
}

func (a *App) reqRollback(peer string, raw json.RawMessage) (bool, error) {
	var m struct{ NovationID string `json:"novation_id"` }
	if json.Unmarshal(raw, &m) != nil { return false, fmt.Errorf("bad payload") }
	nov := a.Novations[m.NovationID]; if nov == nil { return false, fmt.Errorf("rollback: unknown novation %s", m.NovationID) }
	if nov.OutPeer != peer { return false, fmt.Errorf("rollback: wrong peer %s", peer) }
	if nov.Role == "intermediary" { delete(a.Novations, m.NovationID); return true, nil }
	core.Enqueue(&nov.Queue, "rollback"); a.kicks = append(a.kicks, nov.InPeer); return true, nil
}

func (a *App) respRollback(raw json.RawMessage) {
	var m struct{ NovationID string `json:"novation_id"` }; if json.Unmarshal(raw, &m) != nil { return }; delete(a.Novations, m.NovationID)
}

func (a *App) HandleMessage(sender, method string, _ json.RawMessage) bool { return false }

func (a *App) HandleAccountRequest(method string, _ json.RawMessage, _ core.AccountOps) (json.RawMessage, error) {
	return nil, fmt.Errorf("unknown method: %s", method)
}

func (a *App) tryNovate() {
	for debtor := range a.links {
		avlD := a.pbFn(debtor, core.OUT)
		if avlD <= 0 || a.novLinkCount(debtor) >= MaxNovPerLink { continue }
		for creditor := range a.links {
			if creditor == debtor { continue }
			avlC := a.pbFn(creditor, core.IN)
			if avlC <= 0 || a.novLinkCount(creditor) >= MaxNovPerLink { continue }
			amount := uint64(avlD); if uint64(avlC) < amount { amount = uint64(avlC) }; if amount == 0 { continue }
			nonce := make([]byte, 32); rand.Read(nonce); h := sha256.Sum256(nonce); novID := hex.EncodeToString(h[:])
			a.Novations[novID] = &Novation{Amount: amount, Role: "intermediary", InPeer: creditor, OutPeer: debtor, Queue: []string{"commit"}}
			a.kicks = append(a.kicks, debtor)
			avlD -= int64(amount); if avlD <= 0 { break }
		}
	}
}

func SyncMethods() []string    { return []string{"commit", "finalize", "rollback"} }
func MsgMethods() []string     { return nil }
func AccountMethods() []string { return nil }
