// Package loops implements loop clearing with bidirectional probing, 2PC settlement,
// and hash-chain continuation.
//
// Hash chain: seed → h1 → h2 → ... → hN
// Chunk k: id = h(N-k+1). Finalize peels 1: reveal h(N-k). Cancel peels 2: reveal h(N-k-1).
// Intermediary seeing finalize preimage cannot cancel (needs one layer deeper).
// By the time that layer leaks (next finalize), the chunk is already settled.
package loops

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"creditnet/core"
)

const (
	TickRate        = 1000
	MaxPerLink      = 3
	MaxDepth        = 4
	CleanupTimeout  = 60 * time.Second
	CancelTimeout   = 300 // seconds
	PenaltyRate     = 10  // promille
	CleanupRetry    = 5 * time.Minute
	PathTTL         = int64(300)
	SearchInterval  = 10 * time.Minute
	RefractoryDelay = 1 * time.Second
	GasBase         = 16
	ChunkSize       = 1000000
	Nchunks         = 1024
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type SearchMsg struct {
	ID        string `json:"clearing_id"`
	Amount    uint64 `json:"amount"`
	Penalty   uint64 `json:"penalty"`
	Depth     uint8  `json:"depth"`
	Gas       uint64 `json:"gas"`
	Direction uint8  `json:"direction"`
	Hops      uint8  `json:"hops"`
}

type Path struct {
	Peer     [2]string
	Amount   uint64   
	Penalty  uint64   
	Gas      uint64   
	Seed     []byte    // initiator: hash chain seed
	Depth    uint8    
	DepthAck [2]bool  
	Hops     uint8    
	Commit   bool     
}

type ClearingSide struct {
	Account string   `json:"account"`
	Time    int64    `json:"time"`
	Penalty uint64   `json:"penalty,omitempty"`
	Queue   []string `json:"queue,omitempty"`
}

type Clearing struct {
	ID          string          `json:"clearing_id"`
	Amount      uint64          `json:"amount"`
	PenaltyRate uint64          `json:"penalty_rate"`
	Side        [2]ClearingSide `json:"side"`
	Preimage  []byte         // finalize preimage: hash(pre) == id
	CancelPre []byte         // cancel preimage: hash(hash(pre)) == id
	Final     bool           // last chunk — no continuation
	Seed      []byte         // initiator only
	Step      int            // initiator only: position in chain
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

type App struct {
	Clearings map[string]*Clearing `json:"clearings,omitempty"`

	id         string                 
	links      map[string]*core.Link  
	sendFn     core.SendFunc          
	scheduleFn core.ScheduleFunc      
	bwFn       core.BandwidthFunc     
	pbFn       core.PendingBalanceFunc 
	transferFn core.TransferFunc      
	kicks      []string               
	paths      map[string]*Path       
}

func New(nodeID string, links map[string]*core.Link, state json.RawMessage) core.Module {
	a := &App{id: nodeID, links: links}
	if state != nil { json.Unmarshal(state, a) }
	if a.Clearings == nil { a.Clearings = map[string]*Clearing{} }
	a.paths = map[string]*Path{}
	return a
}

func (a *App) SetCallbacks(module string, send core.SendFunc, deferSend core.DeferSendFunc, schedule core.ScheduleFunc, bw core.BandwidthFunc, pb core.PendingBalanceFunc, receipt core.ReceiptFunc, transfer core.TransferFunc) {
	a.sendFn = send; a.scheduleFn = schedule; a.bwFn = bw; a.pbFn = pb; a.transferFn = transfer
}

func (a *App) DrainKicks() []string { k := a.kicks; a.kicks = nil; return k }

func (a *App) ScheduleTimers() {
	if a.scheduleFn != nil { a.scheduleFn(SearchInterval, a.searchAndReschedule) }
	for id, op := range a.Clearings {
		for side := 0; side < 2; side++ {
			if op.Side[side].Account != "" && op.Side[side].Time != 0 &&
				!core.QueueHas(op.Side[side].Queue, "cleanup") {
				a.scheduleCleanup(id, side)
			}
		}
		if op.Seed != nil && op.CancelPre != nil &&
			op.Side[core.IN].Account == "" && op.Side[core.OUT].Account != "" &&
			!core.QueueHas(op.Side[core.OUT].Queue, "cancel") {
			a.scheduleCancel(id)
		}
	}
}

func (a *App) OnRemoveLink(peer string) {
	for id, op := range a.Clearings {
		for s := 0; s < 2; s++ {
			if op.Side[s].Account == peer { op.Side[s] = ClearingSide{} }
		}
		if op.Side[0].Account == "" && op.Side[1].Account == "" { delete(a.Clearings, id) }
	}
}

func (a *App) Status() json.RawMessage {
	return core.Enc(map[string]any{"clearings": len(a.Clearings), "paths": len(a.paths)})
}

// ---------------------------------------------------------------------------
// NextTx / Reserved
// ---------------------------------------------------------------------------

func (a *App) NextTx(peer string) *core.QueuedMsg {
	for id, op := range a.Clearings {
		for side := 0; side < 2; side++ {
			if op.Side[side].Account != peer || core.QueueHead(op.Side[side].Queue) == "" { continue }
			return a.buildMsg(id, op, side)
		}
	}
	return nil
}

func (a *App) Reserved(peer string, side int) uint64 {
	var total uint64
	for _, op := range a.Clearings {
		if op.Side[side].Account == peer { total += op.Amount + op.Side[side].Penalty }
	}
	for _, p := range a.paths {
		if !p.Commit { continue }
		if side == core.IN && p.Peer[0] == peer { total += p.Amount }
		if side == core.OUT && p.Peer[1] == peer { total += p.Amount }
	}
	return total
}

func (a *App) buildMsg(id string, op *Clearing, side int) *core.QueuedMsg {
	switch core.QueueHead(op.Side[side].Queue) {
	case "commit":
		return &core.QueuedMsg{Method: "commit", Payload: core.Enc(map[string]any{"clearing_id": id, "amount": op.Amount, "penalty": op.PenaltyRate})}
	case "finalize":
		m := map[string]any{"clearing_id": id}
		if op.Preimage != nil { m["preimage"] = hex.EncodeToString(op.Preimage) }
		if op.Final { m["final"] = true }
		return &core.QueuedMsg{Method: "finalize", Payload: core.Enc(m)}
	case "cancel":
		m := map[string]any{"clearing_id": id}
		if op.CancelPre != nil { m["preimage"] = hex.EncodeToString(op.CancelPre) }
		return &core.QueuedMsg{Method: "cancel", Payload: core.Enc(m)}
	case "rollback":
		return &core.QueuedMsg{Method: "rollback", Payload: core.Enc(map[string]any{"clearing_id": id})}
	case "cleanup":
		return &core.QueuedMsg{Method: "cleanup", Payload: core.Enc(map[string]any{"clearing_id": id})}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Hash chain helpers
// ---------------------------------------------------------------------------

func penaltyTick(amount, penalty uint64, commitTime, now int64) uint64 {
	if penalty == 0 || commitTime == 0 { return 0 }
	elapsed := now - commitTime; if elapsed < 0 { elapsed = 0 }
	tick := uint64(elapsed+1) * TickRate
	if tick > amount { tick = amount }
	return penalty * tick / amount
}

func chainHash(data []byte, n int) []byte {
	h := make([]byte, len(data))
	copy(h, data)
	for i := 0; i < n; i++ {
		s := sha256.Sum256(h)
		h = s[:]
	}
	return h
}

func hashEq(pre []byte, id string) bool {
	h := sha256.Sum256(pre)
	return hex.EncodeToString(h[:]) == id
}

func doubleHashEq(pre []byte, id string) bool {
	h1 := sha256.Sum256(pre)
	h2 := sha256.Sum256(h1[:])
	return hex.EncodeToString(h2[:]) == id
}

// ---------------------------------------------------------------------------
// 2PC Settlement
// ---------------------------------------------------------------------------

func (a *App) startClearing(id string, amount uint64, outPeer string, preimage, cancelPre, seed []byte, step int) {
	pen := amount * PenaltyRate / 1000
	a.Clearings[id] = &Clearing{
		ID: id, Amount: amount, PenaltyRate: pen,
		Preimage: preimage, CancelPre: cancelPre,
		Final: step <= 2,
		Seed: seed, Step: step,
		Side: [2]ClearingSide{{}, {Account: outPeer, Penalty: pen, Queue: []string{"commit"}}},
	}
	a.kicks = append(a.kicks, outPeer)
}

func (a *App) reqCommit(peer string, ls *core.Link, id string, amount, penalty uint64, ts int64) (bool, error) {
	if amount == 0 || amount > uint64(math.MaxInt64) { return false, nil }

	// Loop closure: initiator receives commit back.
	if op := a.Clearings[id]; op != nil {
		if op.Side[core.IN].Account != "" { return false, nil }
		// Initiator's IN hops: path from search.
		p := a.paths[id]
		var hops uint8
		if p != nil { hops = p.Hops; delete(a.paths, id) }
		inPen := penalty * uint64(hops+1)
		total := amount + inPen
		if a.bwFn(peer, core.IN) < total { return false, nil }
		op.Side[core.IN] = ClearingSide{Account: peer, Time: ts, Penalty: inPen}
		a.scheduleCleanup(id, core.IN)
		core.Enqueue(&op.Side[core.OUT].Queue, "finalize")
		a.kicks = append(a.kicks, op.Side[core.OUT].Account)
		return true, nil
	}

	// Intermediate: read hops from path.
	p := a.paths[id]
	if p == nil { return false, nil }
	hops := p.Hops
	outPeer := p.Peer[1]
	delete(a.paths, id)

	inPen := penalty * uint64(hops+1)
	total := amount + inPen
	if a.bwFn(peer, core.IN) < total { return false, nil }

	outPen := penalty * uint64(hops)
	op := &Clearing{ID: id, Amount: amount, PenaltyRate: penalty, Side: [2]ClearingSide{{Account: peer, Time: ts, Penalty: inPen}, {}}}
	a.Clearings[id] = op
	a.scheduleCleanup(id, core.IN)

	if outPeer != "" {
		op.Side[core.OUT] = ClearingSide{Account: outPeer, Penalty: outPen, Queue: []string{"commit"}}
		a.kicks = append(a.kicks, outPeer)
	}
	return true, nil
}

func (a *App) respCommit(id string, ls *core.Link, ok bool, ts int64) {
	op := a.Clearings[id]
	if op == nil { return }
	core.Dequeue(&op.Side[core.OUT].Queue)
	if ok {
		op.Side[core.OUT].Time = ts
		a.scheduleCleanup(id, core.OUT)
		if op.Seed != nil && op.CancelPre != nil {
			a.scheduleCancel(id)
		}
	} else {
		op.Side[core.OUT].Account = ""
		if op.Side[core.IN].Account != "" {
			core.Enqueue(&op.Side[core.IN].Queue, "rollback")
			a.kicks = append(a.kicks, op.Side[core.IN].Account)
		} else {
			delete(a.Clearings, id)
		}
	}
}

func (a *App) reqFinalize(id, peer string, ls *core.Link, ts int64, preimage []byte, final bool) (bool, error) {
	op := a.Clearings[id]
	if op == nil { return false, nil }
	if op.Side[core.IN].Account != peer { return false, nil }

	// Verify: hash(preimage) == id.
	if len(preimage) == 0 || !hashEq(preimage, id) { return false, fmt.Errorf("bad preimage") }

	// Settle IN side: amount + penalty consumed so far.
	penConsumed := penaltyTick(op.Amount, op.Side[core.IN].Penalty, op.Side[core.IN].Time, ts)
	ls.AddBalance(int64(op.Amount+penConsumed), ts)
	outPeer := op.Side[core.OUT].Account

	// Continuation: create next chunk Clearing before clearing sides.
	// No commit needed — finalize IS the transition to next chunk.
	if !final && len(preimage) > 0 && outPeer != "" {
		nextID := hex.EncodeToString(preimage)
		if a.Clearings[nextID] == nil {
			nextOp := &Clearing{
				ID: nextID, Amount: op.Amount, PenaltyRate: op.PenaltyRate,
				Side: [2]ClearingSide{
					{Account: peer, Time: ts, Penalty: op.Side[core.IN].Penalty},
					{Account: outPeer, Penalty: op.Side[core.OUT].Penalty},
				},
			}
			// Initiator: copy seed, compute next preimages, queue finalize.
			if op.Seed != nil {
				nextStep := op.Step - 1
				nextOp.Seed = op.Seed
				nextOp.Step = nextStep
				if nextStep >= 1 {
					nextOp.Preimage = chainHash(op.Seed, nextStep-1)
				}
				if nextStep >= 2 {
					nextOp.CancelPre = chainHash(op.Seed, nextStep-2)
				}
				// Final if chain exhausted or not enough bandwidth for another chunk.
				avl := a.bwFn(outPeer, core.OUT)
				nextOp.Final = nextStep <= 2 || avl < nextOp.Amount
				core.Enqueue(&nextOp.Side[core.OUT].Queue, "finalize")
				a.kicks = append(a.kicks, outPeer)
			}
			a.Clearings[nextID] = nextOp
			a.scheduleCleanup(nextID, core.IN)
		}
	}

	// Forward finalize to OUT, then clean up current.
	op.Side[core.IN].Account = ""
	if outPeer != "" && len(op.Side[core.OUT].Queue) == 0 {
		core.Enqueue(&op.Side[core.OUT].Queue, "finalize")
		op.Preimage = preimage
		op.Final = final
		a.kicks = append(a.kicks, outPeer)
	}
	if op.Side[core.IN].Account == "" && op.Side[core.OUT].Account == "" {
		delete(a.Clearings, id)
	}
	return true, nil
}

func (a *App) respFinalize(id string, ls *core.Link, ok bool, ts int64) {
	op := a.Clearings[id]
	if op == nil { return }
	outPeer := op.Side[core.OUT].Account
	core.Dequeue(&op.Side[core.OUT].Queue)
	if ok {
		penConsumed := penaltyTick(op.Amount, op.Side[core.OUT].Penalty, op.Side[core.OUT].Time, ts)
		ls.AddBalance(-int64(op.Amount+penConsumed), ts)
		op.Side[core.OUT].Account = ""
		if op.Side[core.IN].Account == "" { delete(a.Clearings, id) }
		if outPeer != "" { a.kicks = append(a.kicks, outPeer) }
	} else {
		op.Side[core.OUT].Account = ""
		if op.Side[core.IN].Account != "" {
			core.Enqueue(&op.Side[core.IN].Queue, "rollback")
			a.kicks = append(a.kicks, op.Side[core.IN].Account)
		} else {
			delete(a.Clearings, id)
		}
	}
}

func (a *App) scheduleCancel(id string) {
	a.scheduleFn(time.Duration(CancelTimeout)*time.Second, func() {
		op := a.Clearings[id]
		if op == nil { return }
		if op.Seed == nil || op.CancelPre == nil { return }
		if op.Side[core.IN].Account != "" { return } // loop completed, no cancel needed
		if op.Side[core.OUT].Account == "" { return }
		core.Enqueue(&op.Side[core.OUT].Queue, "cancel")
		a.kicks = append(a.kicks, op.Side[core.OUT].Account)
	})
}

func (a *App) reqRollback(id, peer string, ls *core.Link, ts int64) (bool, error) {
	op := a.Clearings[id]
	if op == nil { return false, fmt.Errorf("rollback: unknown payment %s", id) }
	if op.Side[core.IN].Account != peer { return false, fmt.Errorf("rollback: wrong peer %s", peer) }

	penConsumed := penaltyTick(op.Amount, op.Side[core.IN].Penalty, op.Side[core.IN].Time, ts)
	if penConsumed > 0 { ls.AddBalance(int64(penConsumed), ts) }
	op.Side[core.IN].Account = ""
	if op.Side[core.OUT].Account != "" {
		core.Enqueue(&op.Side[core.OUT].Queue, "rollback")
		a.kicks = append(a.kicks, op.Side[core.OUT].Account)
	} else {
		delete(a.Clearings, id)
	}
	return true, nil
}

func (a *App) respRollback(id string, ls *core.Link, ok bool, ts int64) {
	op := a.Clearings[id]
	if op == nil { return }
	core.Dequeue(&op.Side[core.OUT].Queue)
	penConsumed := penaltyTick(op.Amount, op.Side[core.OUT].Penalty, op.Side[core.OUT].Time, ts)
	if penConsumed > 0 { ls.AddBalance(-int64(penConsumed), ts) }
	op.Side[core.OUT].Account = ""
	if op.Side[core.IN].Account == "" { delete(a.Clearings, id) }
}

func (a *App) reqCancel(id, peer string, ls *core.Link, ts int64, preimage []byte) (bool, error) {
	op := a.Clearings[id]
	if op == nil { return false, nil }
	if op.Side[core.IN].Account != peer { return false, nil }

	// Verify: hash(hash(preimage)) == id. Cancel peels 2 layers.
	if len(preimage) == 0 || !doubleHashEq(preimage, id) { return false, fmt.Errorf("bad preimage") }

	penConsumed := penaltyTick(op.Amount, op.Side[core.IN].Penalty, op.Side[core.IN].Time, ts)
	if penConsumed > 0 { ls.AddBalance(int64(penConsumed), ts) }
	op.Side[core.IN].Account = ""
	if op.Side[core.OUT].Account != "" {
		core.Enqueue(&op.Side[core.OUT].Queue, "cancel")
		op.CancelPre = preimage // forward same preimage
		a.kicks = append(a.kicks, op.Side[core.OUT].Account)
	} else {
		delete(a.Clearings, id)
	}
	return true, nil
}

func (a *App) respCancel(id string, ls *core.Link, ok bool, ts int64) {
	op := a.Clearings[id]
	if op == nil { return }
	core.Dequeue(&op.Side[core.OUT].Queue)
	if !ok { return }
	penConsumed := penaltyTick(op.Amount, op.Side[core.OUT].Penalty, op.Side[core.OUT].Time, ts)
	if penConsumed > 0 { ls.AddBalance(-int64(penConsumed), ts) }
	op.Side[core.OUT].Account = ""
	if op.Side[core.IN].Account == "" { delete(a.Clearings, id) }
}

func (a *App) reqCleanup(id, peer string, ls *core.Link, ts int64) (bool, error) {
	op := a.Clearings[id]
	if op == nil { return false, nil }
	var settled bool
	var side int
	for s := 0; s < 2; s++ {
		if op.Side[s].Account != peer || op.Side[s].Time == 0 { continue }
		elapsed := ts - op.Side[s].Time
		if elapsed < 0 { continue }
		consumed := uint64(elapsed) * TickRate
		if consumed < op.Amount { continue }
		total := int64(op.Amount + op.Side[s].Penalty)
		if s == core.IN { ls.AddBalance(total, ts) } else { ls.AddBalance(-total, ts) }
		op.Side[s].Account = ""
		settled = true; side = s
	}
	if !settled { return false, nil }
	other := 1 - side
	if len(op.Side[other].Queue) == 0 && op.Side[other].Account != "" {
		core.Enqueue(&op.Side[other].Queue, "cleanup")
		a.kicks = append(a.kicks, op.Side[other].Account)
	}
	if op.Side[0].Account == "" && op.Side[1].Account == "" { delete(a.Clearings, id) }
	return true, nil
}

func (a *App) respCleanup(id, peer string, ls *core.Link, ok bool, ts int64) {
	op := a.Clearings[id]
	if op == nil { return }
	for si := 0; si < 2; si++ {
		if op.Side[si].Account == peer { core.Dequeue(&op.Side[si].Queue) }
	}
	if ok {
		for si := 0; si < 2; si++ {
			if op.Side[si].Account == peer {
				total := int64(op.Amount + op.Side[si].Penalty)
			if si == core.IN { ls.AddBalance(total, ts) } else { ls.AddBalance(-total, ts) }
				op.Side[si].Account = ""
			}
		}
		if op.Side[0].Account == "" && op.Side[1].Account == "" { delete(a.Clearings, id) }
	} else {
		a.scheduleFn(CleanupRetry, func() {
			op := a.Clearings[id]
			if op == nil { return }
			for si := 0; si < 2; si++ {
				if op.Side[si].Account == peer && len(op.Side[si].Queue) == 0 {
					core.Enqueue(&op.Side[si].Queue, "cleanup")
					a.kicks = append(a.kicks, peer)
				}
			}
		})
	}
}

func (a *App) scheduleCleanup(id string, side int) {
	op := a.Clearings[id]
	if op == nil { return }
	t := op.Side[side].Time
	if t == 0 { return }
	elapsed := time.Now().Unix() - t
	remaining := int64(op.Amount/TickRate) - elapsed
	if remaining < 0 { remaining = 0 }
	a.scheduleFn(time.Duration(remaining+1)*time.Second, func() {
		op := a.Clearings[id]
		if op == nil { return }
		peer := op.Side[side].Account
		if peer == "" { return }
		core.Enqueue(&op.Side[side].Queue, "cleanup")
		a.kicks = append(a.kicks, peer)
	})
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

func (a *App) HandleRequest(peer string, ls *core.Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	var m struct {
		ID       string `json:"clearing_id"`
		Amount   uint64 `json:"amount"`
		Penalty  uint64 `json:"penalty"`
		Preimage string `json:"preimage"`
		Final    bool   `json:"final"`
	}
	if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
	switch method {
	case "commit":   return a.reqCommit(peer, ls, m.ID, m.Amount, m.Penalty, ts)
	case "finalize":
		var pre []byte
		if m.Preimage != "" { pre, _ = hex.DecodeString(m.Preimage) }
		return a.reqFinalize(m.ID, peer, ls, ts, pre, m.Final)
	case "cancel":
		var pre []byte
		if m.Preimage != "" { pre, _ = hex.DecodeString(m.Preimage) }
		return a.reqCancel(m.ID, peer, ls, ts, pre)
	case "rollback": return a.reqRollback(m.ID, peer, ls, ts)
	case "cleanup":  return a.reqCleanup(m.ID, peer, ls, ts)
	}
	return false, fmt.Errorf("unknown method: %s", method)
}

func (a *App) HandleResponse(peer string, ls *core.Link, item core.QueuedMsg, ok bool, ts int64) {
	var m struct{ ID string `json:"clearing_id"` }
	if json.Unmarshal(item.Payload, &m) != nil { return }
	switch item.Method {
	case "commit":   a.respCommit(m.ID, ls, ok, ts)
	case "finalize": a.respFinalize(m.ID, ls, ok, ts)
	case "cancel":   a.respCancel(m.ID, ls, ok, ts)
	case "rollback": a.respRollback(m.ID, ls, ok, ts)
	case "cleanup":  a.respCleanup(m.ID, peer, ls, ok, ts)
	}
}

// ---------------------------------------------------------------------------
// Messages — bidirectional flood
// ---------------------------------------------------------------------------

func (a *App) HandleMessage(sender, method string, payload json.RawMessage) bool {
	switch method {
	case "search":  a.msgSearch(sender, payload)
	case "recurse": a.msgRecurse(sender, payload)
	case "found":   a.msgFound(sender, payload)
	case "prepare": a.msgPrepare(sender, payload)
	}
	return false
}

func (a *App) msgSearch(sender string, raw json.RawMessage) {
	var m SearchMsg
	if json.Unmarshal(raw, &m) != nil || m.Amount == 0 { return }
	dir := m.Direction
	if dir > 1 { return }

	p := a.paths[m.ID]
	if p != nil {
		if p.Peer[dir] != "" {
			if m.Gas > 0 && !(p.Peer[0] != "" && p.Peer[1] != "") {
				p.Gas += m.Gas
				var peers []string
				for peer := range a.links {
					if peer == sender { continue }
					if a.linkSlots(peer) >= MaxPerLink { continue }
					if dir == 0 && a.pbFn(peer, core.OUT) > 0 { peers = append(peers, peer) }
					if dir == 1 && a.pbFn(peer, core.IN) > 0 { peers = append(peers, peer) }
				}
				a.floodSearch(m.ID, p, peers, dir, m.Depth, m.Gas)
			}
			return
		}
		if p.Peer[1-dir] == sender { return }
		p.Peer[dir] = sender
		if m.Amount < p.Amount { p.Amount = m.Amount }
		if p.Peer[0] != "" {
			a.sendFn(p.Peer[0], "found", core.Enc(map[string]any{"clearing_id": m.ID, "hops": p.Hops}))
		}
		return
	}

	if m.Penalty*2000 < m.Amount*PenaltyRate { return }
	p = &Path{Amount: m.Amount, Penalty: m.Penalty, Hops: m.Hops, Depth: 1}
	p.Peer[dir] = sender
	a.paths[m.ID] = p
	a.schedulePathExpiry(m.ID)
	a.sendFn(sender, "recurse", core.Enc(map[string]any{
		"clearing_id": m.ID, "direction": m.Direction, "depth": uint8(0),
	}))
}

func (a *App) msgRecurse(sender string, raw json.RawMessage) {
	var m struct {
		ID        string `json:"clearing_id"`
		Direction uint8  `json:"direction"`
		Depth     uint8  `json:"depth"`
	}
	if json.Unmarshal(raw, &m) != nil { return }
	p := a.paths[m.ID]
	if p == nil || (p.Peer[0] != "" && p.Peer[1] != "") { return }
	if m.Depth+1 != p.Depth { return }

	if p.Seed != nil {
		if p.DepthAck[m.Direction] { return }
		p.DepthAck[m.Direction] = true
		if !p.DepthAck[0] || !p.DepthAck[1] { return }
		if p.Depth >= MaxDepth { return }
		p.Gas += gasForDepth(p.Depth)
		p.Depth++
		p.DepthAck = [2]bool{}
		id := m.ID
		a.scheduleFn(RefractoryDelay, func() {
			p := a.paths[id]
			if p == nil || (p.Peer[0] != "" && p.Peer[1] != "") { return }
			a.floodSearchInitiator(id, p)
		})
		return
	}

	for d := 0; d < 2; d++ {
		if p.Peer[d] == sender { continue }
		if p.Peer[d] != "" {
			a.sendFn(p.Peer[d], "recurse", core.Enc(map[string]any{
				"clearing_id": m.ID, "direction": m.Direction, "depth": p.Depth,
			}))
			return
		}
	}
}

func (a *App) msgFound(sender string, raw json.RawMessage) {
	var m struct {
		ID   string `json:"clearing_id"`
		Hops uint8  `json:"hops"`
	}
	if json.Unmarshal(raw, &m) != nil { return }
	p := a.paths[m.ID]
	if p == nil { return }
	p.Hops = m.Hops + 1
	if p.Seed != nil {
		if MaxDepth > 0 && p.Hops > MaxDepth*2+1 { return }
		a.startLoop(m.ID)
		return
	}
	if p.Peer[0] != "" {
		a.sendFn(p.Peer[0], "found", core.Enc(map[string]any{"clearing_id": m.ID, "hops": p.Hops}))
	}
}

func (a *App) msgPrepare(sender string, raw json.RawMessage) {
	var m struct {
		ID     string `json:"clearing_id"`
		Amount uint64 `json:"amount"`
	}
	if json.Unmarshal(raw, &m) != nil || m.Amount == 0 { return }
	p := a.paths[m.ID]
	if p == nil { return }

	if p.Seed != nil {
		// Initiator: prepare came back. Start 2PC.
		if m.Amount > ChunkSize { m.Amount = ChunkSize }
		step := Nchunks
		preimage := chainHash(p.Seed, step-1)  // finalize: peel 1
		cancelPre := chainHash(p.Seed, step-2)  // cancel: peel 2
		a.startClearing(m.ID, m.Amount, p.Peer[1], preimage, cancelPre, p.Seed, step)
		delete(a.paths, m.ID)
		return
	}

	if p.Peer[1] == "" { return }
	if a.bwFn(sender, core.IN) < m.Amount { return }
	avl := a.bwFn(p.Peer[1], core.OUT)
	if avl < m.Amount { m.Amount = avl }
	if m.Amount > ChunkSize { m.Amount = ChunkSize }
	if m.Amount == 0 { return }
	p.Commit = true; p.Amount = m.Amount
	a.sendFn(p.Peer[1], "prepare", core.Enc(map[string]any{"clearing_id": m.ID, "amount": m.Amount}))
}

// ---------------------------------------------------------------------------
// Search helpers
// ---------------------------------------------------------------------------

func (a *App) startLoop(searchID string) {
	p := a.paths[searchID]
	if p == nil || p.Peer[1] == "" { return }
	avl := a.bwFn(p.Peer[1], core.OUT)
	if avl <= 0 { delete(a.paths, searchID); return }
	amount := uint64(avl)
	if p.Amount < amount { amount = p.Amount }
	if amount > ChunkSize { amount = ChunkSize }
	if amount == 0 { delete(a.paths, searchID); return }
	a.sendFn(p.Peer[1], "prepare", core.Enc(map[string]any{"clearing_id": searchID, "amount": amount}))
}

func (a *App) floodSearch(searchID string, p *Path, peers []string, dir uint8, depth uint8, gas uint64) {
	if gas == 0 || len(peers) == 0 { return }
	share := gas / uint64(len(peers))
	if share < 1 { share = 1 }
	for _, peer := range peers {
		if gas < share { break }
		avl := a.pbFn(peer, core.OUT)
		if dir == 1 { avl = a.pbFn(peer, core.IN) }
		if avl <= 0 { continue }
		amount := p.Amount
		if uint64(avl) < amount { amount = uint64(avl) }
		a.sendFn(peer, "search", core.Enc(SearchMsg{
			ID: searchID, Amount: amount, Penalty: p.Penalty, Depth: depth + 1, Gas: share, Direction: dir, Hops: p.Hops + 1,
		}))
		a.transferFn(peer, share)
		gas -= share
	}
}

func (a *App) floodSearchInitiator(searchID string, p *Path) {
	var debtors, creditors []string
	for peer := range a.links {
		if a.linkSlots(peer) >= MaxPerLink { continue }
		if a.pbFn(peer, core.OUT) > 0 { debtors = append(debtors, peer) }
		if a.pbFn(peer, core.IN) > 0 { creditors = append(creditors, peer) }
	}
	a.floodSearch(searchID, p, debtors, 0, 0, p.Gas)
	a.floodSearch(searchID, p, creditors, 1, 0, p.Gas)
}

func (a *App) sendSearches() {
	var debtors, creditors []string
	for peer := range a.links {
		if a.linkSlots(peer) >= MaxPerLink { continue }
		if a.pbFn(peer, core.OUT) > 0 { debtors = append(debtors, peer) }
		if a.pbFn(peer, core.IN) > 0 { creditors = append(creditors, peer) }
	}
	if len(debtors) == 0 || len(creditors) == 0 { return }

	seed := make([]byte, 32)
	rand.Read(seed)
	// Search ID = first payment ID = chainHash(seed, Nchunks).
	searchID := hex.EncodeToString(chainHash(seed, Nchunks))

	var amount uint64
	for _, peer := range debtors {
		avl := a.pbFn(peer, core.OUT)
		if avl > 0 && (amount == 0 || uint64(avl) < amount) { amount = uint64(avl) }
	}

	pen := amount * PenaltyRate / 1000
	p := &Path{
		Seed: seed,
		Amount: amount, Penalty: pen,
		Gas: gasForDepth(0), Depth: 1,
	}
	a.paths[searchID] = p
	a.schedulePathExpiry(searchID)
	a.floodSearchInitiator(searchID, p)
}

func (a *App) schedulePathExpiry(id string) {
	a.scheduleFn(time.Duration(PathTTL)*time.Second, func() { delete(a.paths, id) })
}

func (a *App) searchAndReschedule() {
	a.sendSearches()
	if a.scheduleFn != nil { a.scheduleFn(SearchInterval, a.searchAndReschedule) }
}

func (a *App) linkSlots(peer string) int {
	count := 0
	for _, op := range a.Clearings {
		if op.Side[core.IN].Account == peer || op.Side[core.OUT].Account == peer { count++ }
	}
	return count
}

// ---------------------------------------------------------------------------
// Account API
// ---------------------------------------------------------------------------

func (a *App) HandleAccountRequest(method string, payload json.RawMessage, ops core.AccountOps) (json.RawMessage, error) {
	if method == "status" { return a.Status(), nil }
	return nil, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func gasForDepth(depth uint8) uint64 {
	g := uint64(1)
	for i := uint8(0); i <= depth; i++ { g *= GasBase }
	return g
}

func SyncMethods() []string    { return []string{"commit", "finalize", "cancel", "rollback", "cleanup"} }
func MsgMethods() []string     { return []string{"search", "recurse", "found", "prepare"} }
func AccountMethods() []string { return []string{"status"} }
