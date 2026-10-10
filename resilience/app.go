// Resilience: flood pathfinding (like Ripple) with fee redistribution.
// After settlement, earned fees are redistributed to peers with positive
// balance, weighted by each peer's width — a running average of the tax
// rate on payments that moved the balance away from zero.
package resilience

import (
	"encoding/json"
	"fmt"
	"time"

	"creditnet/core"
	"creditnet/jlib"
)

const (
	MaxDepth       = 4
	DefaultFeeRate = 10
	PenaltyRate    = 10
	GasBase        = 16
)

type directEntry struct {
	Amount uint64 `json:"amount"`
	Tax    uint64 `json:"tax"`
}

type App struct {
	Engine       *jlib.Engine       `json:"engine"`
	Width        map[string]float64 `json:"width"`
	taxBuffer    map[string]uint64
	directOutbox map[string]directEntry
	FeeRate      uint64             `json:"fee_rate"`

	id         string
	links      map[string]*core.Link
	sendFn     core.SendFunc
	bwFn       core.BandwidthFunc
	pbFn       core.PendingBalanceFunc
	receiptFn  core.ReceiptFunc
	kicks      []string
}

func New(nodeID string, links map[string]*core.Link, state json.RawMessage) core.Module {
	a := &App{id: nodeID, links: links, Width: map[string]float64{}, taxBuffer: map[string]uint64{}, directOutbox: map[string]directEntry{}}
	if state != nil { json.Unmarshal(state, a) }
	if a.Width == nil { a.Width = map[string]float64{} }
	if a.taxBuffer == nil { a.taxBuffer = map[string]uint64{} }
	if a.directOutbox == nil { a.directOutbox = map[string]directEntry{} }
	if a.FeeRate == 0 { a.FeeRate = DefaultFeeRate }
	return a
}

func (a *App) SetCallbacks(module string, send core.SendFunc, deferSend core.DeferSendFunc, schedule core.ScheduleFunc, bw core.BandwidthFunc, pb core.PendingBalanceFunc, receipt core.ReceiptFunc, transfer core.TransferFunc) {
	a.sendFn = send
	a.bwFn = bw
	a.pbFn = pb
	a.receiptFn = receipt
	if a.Engine == nil {
		a.Engine = jlib.NewEngine(a.id, module, send, deferSend, schedule, bw, receipt)
	} else {
		a.Engine.Bind(a.id, module, send, deferSend, schedule, bw, receipt)
	}
	a.Engine.GasCharge = 1
	a.Engine.FeeRate = a.FeeRate
	a.Engine.PenaltyRate = PenaltyRate
	a.Engine.SellerFee = true
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

func (a *App) DrainKicks() []string {
	kicks := a.Engine.DrainKicks()
	kicks = append(kicks, a.kicks...)
	a.kicks = nil
	for peer := range a.taxBuffer {
		kicks = append(kicks, peer)
	}
	return kicks
}
func (a *App) ScheduleTimers()                         { a.Engine.ScheduleTimers() }
func (a *App) NextTx(peer string) *core.QueuedMsg {
	if e, ok := a.directOutbox[peer]; ok {
		return &core.QueuedMsg{Method: "direct_payment", Payload: core.Enc(map[string]uint64{"amount": e.Amount, "tax": e.Tax})}
	}
	if amount := a.taxBuffer[peer]; amount > 0 {
		return &core.QueuedMsg{Method: "redistribution", Payload: core.Enc(map[string]uint64{"amount": amount})}
	}
	return a.Engine.NextTx(peer)
}
func (a *App) Reserved(peer string, side int) uint64   { return a.Engine.Reserved(peer, side) }

func (a *App) OnRemoveLink(peer string) {
	a.Engine.OnRemoveLink(peer)
	delete(a.Width, peer)
	delete(a.taxBuffer, peer)
}

func (a *App) Status() json.RawMessage {
	return core.Enc(map[string]any{"payments": len(a.Engine.Payments), "paths": len(a.Engine.Paths), "width": a.Width, "fee_rate": a.FeeRate})
}

func SyncMethods() []string     { return append(jlib.SyncMethods(), "redistribution", "direct_payment") }
func MsgMethods() []string      { return jlib.MsgMethods() }
func OpenMsgMethods() []string  { return jlib.OpenMsgMethods() }
func AccountMethods() []string  { return []string{"send_payment", "accept_payment", "pending_requests", "local_payment", "set_fee_rate", "status"} }

// ---------------------------------------------------------------------------
// Width tracking
// ---------------------------------------------------------------------------

func (a *App) taxRate() float64 {
	return float64(a.FeeRate) / float64(1000+a.FeeRate)
}

// updateWidth mirrors Resilience C: update width when balance moves away
// from zero. delta is the balance change that was just applied. rate is
// the actual tax rate from the payment, not the node's own rate.
func (a *App) updateWidth(peer string, delta int64, rate float64) {
	l := a.links[peer]
	if l == nil { return }
	bal := l.Balance
	old := bal - delta

	// Only update when moving away from zero.
	if !((delta > 0 && bal > 0) || (delta < 0 && bal < 0)) { return }

	w := a.Width[peer]
	if (old > 0) == (bal > 0) {
		weight := float64(old) / float64(bal)
		a.Width[peer] = weight*w + (1-weight)*rate
	} else {
		a.Width[peer] = rate
	}
}

// ---------------------------------------------------------------------------
// Redistribute
// ---------------------------------------------------------------------------

// redistribute spreads earned fee to peers with positive balance, weighted
// by width. Accumulates in taxBuffer; sent as "redistribution" sync.
func (a *App) redistribute(fromPeer string, fee uint64, tr float64) {
	if fee == 0 { return }
	var totalWidth float64
	type candidate struct {
		peer string
		w    float64
		bal  uint64
	}
	var cands []candidate

	for peer := range a.links {
		if peer == fromPeer { continue }
		bal := a.pbFn(peer, core.OUT)
		if bal <= 0 { continue }
		w := a.Width[peer]
		if w <= 0 { continue }
		cands = append(cands, candidate{peer, w, uint64(bal)})
		totalWidth += w
	}
	if totalWidth == 0 { return }

	amount := fee
	if totalWidth < tr {
		amount = uint64(float64(fee) * (totalWidth / tr))
	}
	if amount == 0 { return }

	for _, c := range cands {
		share := uint64(float64(amount) * (c.w / totalWidth))
		if share == 0 { continue }
		if share > c.bal { share = c.bal }
		a.taxBuffer[c.peer] += share
	}
}



// ---------------------------------------------------------------------------
// Settlement helpers — capture fee before Engine may delete payment
// ---------------------------------------------------------------------------

type settlementInfo struct {
	amount uint64
	inFee  uint64
	outFee uint64
	inPen  uint64
	found  bool
}

func (a *App) captureSettlement(id string) settlementInfo {
	p := a.Engine.Payments[id]
	if p == nil { return settlementInfo{} }
	return settlementInfo{
		amount: p.Amount,
		inFee:  p.Side[core.IN].Fee,
		outFee: p.Side[core.OUT].Fee,
		inPen:  p.Side[core.IN].Penalty,
		found:  true,
	}
}

func (a *App) afterSettlement(peer string, ls *core.Link, info settlementInfo, balBefore int64) {
	if !info.found { return }
	delta := ls.Balance - balBefore
	if delta == 0 { return }

	nominalFee := int64(info.inFee) - int64(info.outFee)
	rate := float64(0)
	if nominalFee > 0 && info.amount > 0 {
		rate = float64(nominalFee) / float64(int64(info.amount)+nominalFee)
	}
	a.updateWidth(peer, delta, rate)
	if nominalFee > 0 {
		// Scale by actual/nominal: delta is actual total, amount+inFee+inPen is nominal total.
		nominal := int64(info.amount + info.inFee + info.inPen)
		actualFee := nominalFee
		if nominal > 0 && delta < nominal {
			actualFee = nominalFee * delta / nominal
		}
		if actualFee > 0 {
			a.redistribute(peer, uint64(actualFee), rate)
		}
	}
}

// ---------------------------------------------------------------------------
// HandleMessage — passthrough
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// HandleRequest — Engine + width tracking
// ---------------------------------------------------------------------------

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
		info := a.captureSettlement(id)
		balBefore := ls.Balance
		ok, err := a.Engine.ReqFinalize(id, peer, ls, ts, pre)
		if ok { a.afterSettlement(peer, ls, info, balBefore) }
		return ok, err
	case "cancel":
		var m struct{ Preimage string `json:"preimage"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		pre, _ := jlib.DecodeHex(m.Preimage)
		id := jlib.HashHex(pre)
		info := a.captureSettlement(id)
		balBefore := ls.Balance
		ok, err := a.Engine.ReqCancel(id, peer, ls, ts, pre)
		if ok { a.afterSettlement(peer, ls, info, balBefore) }
		return ok, err
	case "cleanup":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		info := a.captureSettlement(m.PaymentID)
		balBefore := ls.Balance
		ok, err := a.Engine.ReqCleanup(m.PaymentID, peer, ls, ts)
		if ok { a.afterSettlement(peer, ls, info, balBefore) }
		return ok, err
	case "rollback":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		return a.Engine.ReqRollback(m.PaymentID, peer)
	case "direct_payment":
		var m struct{ Amount uint64 `json:"amount"`; Tax uint64 `json:"tax"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		total := m.Amount + m.Tax
		if a.bwFn(peer, core.IN) < total { return false, nil }
		balBefore := ls.Balance
		ls.AddBalance(int64(total), ts)
		rate := float64(0)
		if total > 0 { rate = float64(m.Tax) / float64(total) }
		a.updateWidth(peer, ls.Balance-balBefore, rate)
		if m.Tax > 0 { a.redistribute(peer, m.Tax, rate) }
		if a.receiptFn != nil { a.receiptFn("", peer, a.Engine.Module(), int64(m.Amount), true) }
		return true, nil
	case "redistribution":
		var m struct{ Amount uint64 `json:"amount"` }
		if json.Unmarshal(payload, &m) != nil { return false, fmt.Errorf("bad payload") }
		oldBal := ls.Balance
		ls.AddBalance(int64(m.Amount), ts)
		// Cascade: if this reduced negative balance, redistribute onward.
		// Use existing width as rate — matches C direct_transfer.
		if oldBal < 0 {
			out := m.Amount
			if ls.Balance > 0 { out = uint64(-oldBal) }
			a.redistribute(peer, out, a.Width[peer])
		}
		return true, nil
	}
	return false, nil
}

// ---------------------------------------------------------------------------
// HandleResponse — Engine + width tracking + redistribute
// ---------------------------------------------------------------------------

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
		info := a.captureSettlement(id)
		balBefore := ls.Balance
		a.Engine.RespFinalize(id, ls, ok, ts)
		if ok { a.afterSettlement(peer, ls, info, balBefore) }
	case "cancel":
		var m struct{ Preimage string `json:"preimage"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		pre, _ := jlib.DecodeHex(m.Preimage)
		id := jlib.HashHex(pre)
		info := a.captureSettlement(id)
		balBefore := ls.Balance
		a.Engine.RespCancel(id, ls, ok, ts)
		if ok { a.afterSettlement(peer, ls, info, balBefore) }
	case "cleanup":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		info := a.captureSettlement(m.PaymentID)
		balBefore := ls.Balance
		a.Engine.RespCleanup(m.PaymentID, peer, ls, ok, ts)
		if ok { a.afterSettlement(peer, ls, info, balBefore) }
	case "rollback":
		var m struct{ PaymentID string `json:"payment_id"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		a.Engine.RespRollback(m.PaymentID, ok)
	case "direct_payment":
		var m struct{ Amount uint64 `json:"amount"`; Tax uint64 `json:"tax"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		delete(a.directOutbox, peer)
		total := int64(m.Amount + m.Tax)
		if ok {
			rate := float64(0)
			if m.Amount+m.Tax > 0 { rate = float64(m.Tax) / float64(m.Amount+m.Tax) }
			ls.AddBalance(-total, ts)
			a.updateWidth(peer, -total, rate)
		}
		if a.receiptFn != nil {
			a.receiptFn("", peer, a.Engine.Module(), -int64(m.Amount), ok)
		}
	case "redistribution":
		var m struct{ Amount uint64 `json:"amount"` }
		if json.Unmarshal(item.Payload, &m) != nil { return }
		if ok {
			ls.AddBalance(-int64(m.Amount), ts)
		}
		if a.taxBuffer[peer] <= m.Amount {
			delete(a.taxBuffer, peer)
		} else {
			a.taxBuffer[peer] -= m.Amount
		}
	}
}

// ---------------------------------------------------------------------------
// Account API
// ---------------------------------------------------------------------------

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
	case "local_payment":
		var m struct{ Peer string `json:"peer"`; Amount uint64 `json:"amount"` }
		if json.Unmarshal(payload, &m) != nil || m.Amount == 0 { return nil, fmt.Errorf("bad payload") }
		if a.links[m.Peer] == nil { return nil, fmt.Errorf("no link to %s", m.Peer) }
		if _, ok := a.directOutbox[m.Peer]; ok { return nil, fmt.Errorf("already pending") }
		tax := uint64(float64(m.Amount) * a.taxRate())
		a.directOutbox[m.Peer] = directEntry{Amount: m.Amount, Tax: tax}
		a.kicks = append(a.kicks, m.Peer)
		return core.Enc("ok"), nil
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
