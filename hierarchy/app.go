// Hierarchy: tree topology, linear pathfinder, shared settlement.
package hierarchy

import (
	"encoding/json"
	"fmt"

	"creditnet/core"
	"creditnet/jlib"
)

const (
	MaxDepth       = 8
	DefaultFeeRate = 10
	GasPerHop      = 1024
)

type App struct {
	Engine  *jlib.Engine `json:"engine"`
	Parent  string       `json:"parent"`
	FeeRate uint64       `json:"fee_rate"`

	id     string
	links  map[string]*core.Link
	sendFn core.SendFunc
}

func New(nodeID string, links map[string]*core.Link, state json.RawMessage) core.Module {
	a := &App{id: nodeID, links: links}
	if state != nil { json.Unmarshal(state, a) }
	if a.FeeRate == 0 { a.FeeRate = DefaultFeeRate }
	return a
}

func (a *App) SetCallbacks(module string, send core.SendFunc, deferSend core.DeferSendFunc, schedule core.ScheduleFunc, bw core.BandwidthFunc, pb core.PendingBalanceFunc, receipt core.ReceiptFunc, transfer core.TransferFunc) {
	a.sendFn = send
	if a.Engine == nil {
		a.Engine = jlib.NewEngine(a.id, module, send, deferSend, schedule, bw, receipt)
	} else {
		a.Engine.Bind(a.id, module, send, deferSend, schedule, bw, receipt)
	}
	a.Engine.Linear = true
	a.Engine.FeeRate = a.FeeRate
	a.Engine.GasCharge = GasPerHop
	a.Engine.MaxDepth = MaxDepth
	a.Engine.TransferFn = transfer
	a.Engine.ParentFn = func() string { return a.Parent }
	a.Engine.GasPerRound = func(depth uint8) uint64 { return GasPerHop }
}

func (a *App) DrainKicks() []string { return a.Engine.DrainKicks() }
func (a *App) ScheduleTimers()           { a.Engine.ScheduleTimers() }
func (a *App) OnRemoveLink(peer string) {
	if peer == a.Parent { a.Parent = "" }
	a.Engine.OnRemoveLink(peer)
}
func (a *App) NextTx(peer string) *core.QueuedMsg { return a.Engine.NextTx(peer) }
func (a *App) Reserved(peer string, side int) uint64 { return a.Engine.Reserved(peer, side) }
func (a *App) Status() json.RawMessage {
	return core.Enc(map[string]any{"payments": len(a.Engine.Payments), "paths": len(a.Engine.Paths), "parent": a.Parent, "fee_rate": a.FeeRate})
}

func SyncMethods() []string     { return jlib.SyncMethods() }
func MsgMethods() []string      { return jlib.MsgMethods() }
func OpenMsgMethods() []string  { return jlib.OpenMsgMethods() }
func AccountMethods() []string  { return []string{"send_payment", "accept_payment", "pending_requests", "set_parent", "set_fee_rate", "status"} }

func (a *App) HandleMessage(sender, method string, payload json.RawMessage) bool {
	switch method {
	case "payment_request":    a.Engine.MsgPaymentRequest(sender, payload)
	case "payment_accept":     a.Engine.MsgPaymentAccept(sender, payload)
	case "search":             a.Engine.MsgSearch(sender, payload)
	case "recurse":            a.Engine.MsgRecurse(sender, payload)
	case "found":              a.Engine.MsgFound(sender, payload)
	case "prepare":            a.Engine.MsgPrepare(sender, payload)
	case "counterpart_search": a.Engine.MsgCounterpartSearch(sender, payload)
	case "counterpart_commit": a.Engine.MsgCounterpartCommit(sender, payload)
	case "counterpart_seal":   a.Engine.MsgCounterpartSeal(sender, payload)
	}
	return false
}

func (a *App) HandleRequest(peer string, ls *core.Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "commit":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		return a.Engine.ReqCommit(m.PaymentID, peer, ls, ts)
	case "seal":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		return a.Engine.ReqSeal(m.PaymentID, peer, ts)
	case "finalize":
		var m struct{ Preimage string `json:"preimage"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		pre, _ := jlib.DecodeHex(m.Preimage)
		id := jlib.DoubleHashHex(pre)
		return a.Engine.ReqFinalize(id, peer, ls, ts, pre)
	case "cancel":
		var m struct{ Preimage string `json:"preimage"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		pre, _ := jlib.DecodeHex(m.Preimage)
		id := jlib.HashHex(pre)
		return a.Engine.ReqCancel(id, peer, ls, ts, pre)
	case "cleanup":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		return a.Engine.ReqCleanup(m.PaymentID, peer, ls, ts)
	case "rollback":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		return a.Engine.ReqRollback(m.PaymentID, peer)
	}
	return false, fmt.Errorf("unknown method: %s", method)
}

func (a *App) HandleResponse(peer string, ls *core.Link, item core.QueuedMsg, ok bool, ts int64) {
	switch item.Method {
	case "commit":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		a.Engine.RespCommit(m.PaymentID, ls, ok, ts)
	case "seal":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		a.Engine.RespSeal(m.PaymentID, ok, ts)
	case "finalize":
		var m struct{ Preimage string `json:"preimage"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		pre, _ := jlib.DecodeHex(m.Preimage)
		id := jlib.DoubleHashHex(pre)
		a.Engine.RespFinalize(id, ls, ok, ts)
	case "cancel":
		var m struct{ Preimage string `json:"preimage"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		pre, _ := jlib.DecodeHex(m.Preimage)
		id := jlib.HashHex(pre)
		a.Engine.RespCancel(id, ls, ok, ts)
	case "cleanup":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		a.Engine.RespCleanup(m.PaymentID, peer, ls, ok, ts)
	case "rollback":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		a.Engine.RespRollback(m.PaymentID, ok)
	}
}

func (a *App) HandleAccountRequest(method string, payload json.RawMessage, ops core.AccountOps) (json.RawMessage, error) {
	switch method {
	case "send_payment":
		var m struct{ Target string `json:"target"`; Amount uint64 `json:"amount"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		if a.Parent == "" { return nil, fmt.Errorf("no parent configured") }
		if a.Engine.SendPayment(m.Target, m.Amount, a.sendFn) == "" { return nil, fmt.Errorf("invalid payment") }
		return core.Enc("ok"), nil
	case "accept_payment":
		var m struct{ ID string `json:"id"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		pid, err := a.Engine.AcceptPayment(m.ID, a.sendFn)
		if err != nil { return nil, err }
		return core.Enc(map[string]string{"payment_id": pid}), nil
	case "pending_requests":
		return core.Enc(a.Engine.PendingReqs), nil
	case "set_parent":
		var m struct{ Peer string `json:"peer"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		if m.Peer != "" && a.links[m.Peer] == nil { return nil, fmt.Errorf("no link to %s", m.Peer) }
		a.Parent = m.Peer
		return core.Enc(map[string]string{"parent": a.Parent}), nil
	case "set_fee_rate":
		var m struct{ Rate uint64 `json:"rate"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		a.FeeRate = m.Rate
		a.Engine.FeeRate = m.Rate
		return core.Enc("ok"), nil
	case "status":
		return a.Status(), nil
	}
	return nil, fmt.Errorf("unknown method: %s", method)
}
