package ripple

import (
	"encoding/json"
	"fmt"
	"time"

	"creditnet/core"
	"creditnet/jlib"
)

const (
	MaxDepth = 4; DefaultFeeRate = 10; PenaltyRate = 10
	GasBase = 16
)

type App struct {
	Engine  *jlib.Engine `json:"engine"`
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
	a.Engine.GasCharge = 1
	a.Engine.FeeRate = a.FeeRate
	a.Engine.PenaltyRate = PenaltyRate
	a.Engine.MaxDepth = MaxDepth
	a.Engine.RefractoryDelay = time.Second
	a.Engine.TransferFn = transfer
	a.Engine.PeersFn = func() []string {
		var peers []string
		for p := range a.links { peers = append(peers, p) }
		return peers
	}
	a.Engine.GasPerRound = func(depth uint8) uint64 {
		g := uint64(1)
		for i := uint8(0); i <= depth; i++ { g *= GasBase }
		return g
	}
}

func (a *App) DrainKicks() []string { return a.Engine.DrainKicks() }
func (a *App) ScheduleTimers()           { a.Engine.ScheduleTimers() }
func (a *App) OnRemoveLink(peer string)  { a.Engine.OnRemoveLink(peer) }
func (a *App) NextTx(peer string) *core.QueuedMsg { return a.Engine.NextTx(peer) }
func (a *App) Reserved(peer string, side int) uint64 { return a.Engine.Reserved(peer, side) }
func (a *App) Status() json.RawMessage {
	return core.Enc(map[string]any{"payments": len(a.Engine.Payments), "paths": len(a.Engine.Paths), "fee_rate": a.FeeRate})
}

func SyncMethods() []string     { return jlib.SyncMethods() }
func MsgMethods() []string      { return jlib.MsgMethods() }
func OpenMsgMethods() []string  { return jlib.OpenMsgMethods() }
func AccountMethods() []string  { return []string{"send_payment", "accept_payment", "pending_requests", "set_fee_rate", "status"} }

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
		var m struct{ Recipient string `json:"recipient"`; Amount uint64 `json:"amount"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		if a.Engine.SendPayment(m.Recipient, m.Amount, a.sendFn) == "" { return nil, fmt.Errorf("invalid payment") }
		return core.Enc("ok"), nil
	case "accept_payment":
		var m struct{ ID string `json:"id"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		pid, err := a.Engine.AcceptPayment(m.ID, a.sendFn)
		if err != nil { return nil, err }
		return core.Enc(map[string]string{"payment_id": pid}), nil
	case "pending_requests":
		return core.Enc(a.Engine.PendingReqs), nil
	case "set_fee_rate":
		var m struct{ Rate uint64 `json:"rate"` }
		if json.Unmarshal(payload, &m) != nil { return nil, fmt.Errorf("bad payload") }
		a.FeeRate = m.Rate
		a.Engine.FeeRate = m.Rate
		return core.Enc("ok"), nil
	case "status":
		return a.Status(), nil
	}
	return nil, nil
}
