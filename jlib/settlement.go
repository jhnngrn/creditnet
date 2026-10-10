// 3PC settlement: commit → seal → finalize, with penalty asymmetry.
// Methods on Engine (shared with pathfinder.go).
package jlib

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/bits"
	"time"

	"creditnet/core"
)

const (
	TickRate   = 1000
	RetryDelay = time.Minute
)

type PaymentSide struct {
	Account    string   `json:"account"`
	Fee        uint64   `json:"fee"`
	Penalty    uint64   `json:"penalty"`
	CommitTime int64    `json:"commit_time"`
	SealTime   int64    `json:"seal_time"`
	Queue      []string `json:"queue,omitempty"`
}

type Payment struct {
	ID          string         `json:"payment_id"`
	Amount      uint64         `json:"amount"`
	Counterpart string         `json:"counterpart,omitempty"`
	Side        [2]PaymentSide `json:"side"`
	Preimage    []byte         `json:"preimage,omitempty"`
}

// ---------------------------------------------------------------------------
// Penalty math
// ---------------------------------------------------------------------------

func mulDiv(a, b, c uint64) uint64 {
	if a == 0 || b == 0 || c == 0 { return 0 }
	hi, lo := bits.Mul64(a, b)
	if hi >= c { return math.MaxUint64 }
	q, _ := bits.Div64(hi, lo, c)
	return q
}

func penaltyElapsed(amount uint64, from, to int64) uint64 {
	diff := to - from
	if diff < 0 { diff = 0 }
	ticks := uint64(diff) * TickRate
	if ticks > amount { return amount }
	return ticks
}

func amountFinal(p *Payment, ticker uint64, side int) uint64 {
	phase1 := penaltyElapsed(p.Amount, p.Side[side].CommitTime, p.Side[side].SealTime)
	if phase1 > ticker { phase1 = ticker }
	return p.Amount - (ticker - phase1)
}

func finalizeAmt(p *Payment, ticker uint64, side int) int64 {
	af := amountFinal(p, ticker, side)
	pf := mulDiv(p.Side[side].Penalty, ticker, p.Amount)
	rf := mulDiv(p.Side[side].Fee, af, p.Amount)
	t := af + rf + pf
	if side == core.IN { return int64(t) }
	return -int64(t)
}

func cancelAmt(p *Payment, ts int64, side int) int64 {
	ticker := penaltyElapsed(p.Amount, p.Side[side].CommitTime, ts)
	total := p.Amount + p.Side[side].Fee + p.Side[side].Penalty
	var r uint64
	if ticker >= p.Amount { r = total } else { r = mulDiv(total, ticker, p.Amount) }
	if side == core.IN { return int64(r) }
	return -int64(r)
}

func cleanupAmt(p *Payment, side int) int64 {
	if p.Side[side].SealTime > 0 { return finalizeAmt(p, p.Amount, side) }
	t := p.Amount + p.Side[side].Fee + p.Side[side].Penalty
	if side == core.IN { return int64(t) }
	return -int64(t)
}

func penaltyDuration(amount uint64) time.Duration {
	secs := amount / TickRate
	if amount%TickRate != 0 { secs++ }
	return time.Duration(secs) * time.Second
}

// ---------------------------------------------------------------------------
// StartPayment — endpoint initiates settlement from path
// ---------------------------------------------------------------------------

func (e *Engine) StartPayment(pp *Path) string {
	h := sha256.Sum256(pp.Preimage)
	id := hex.EncodeToString(h[:])
	outFee := pp.Fee * e.feeHops(uint64(pp.Hops))
	outPen := pp.Penalty * uint64(pp.Hops)
	e.Payments[id] = &Payment{
		ID: id, Amount: pp.Amount, Counterpart: pp.Counterpart,
		Preimage: pp.Preimage,
		Side: [2]PaymentSide{
			{},
			{Account: pp.Peer[1], Fee: outFee, Penalty: outPen},
		},
	}
	core.Enqueue(&e.Payments[id].Side[core.OUT].Queue, "commit")
	e.kicks = append(e.kicks, pp.Peer[1])
	e.scheduleCancel(id, time.Duration(CommitTimeout)*time.Second)
	return id
}

// ---------------------------------------------------------------------------
// NextTx
// ---------------------------------------------------------------------------

func (e *Engine) NextTx(peer string) *core.QueuedMsg {
	for id, p := range e.Payments {
		for side := 0; side < 2; side++ {
			if p.Side[side].Account != peer || core.QueueHead(p.Side[side].Queue) == "" { continue }
			return e.buildMsg(id, p, side)
		}
	}
	return nil
}

func (e *Engine) buildMsg(id string, p *Payment, side int) *core.QueuedMsg {
	switch core.QueueHead(p.Side[side].Queue) {
	case "commit":
		return &core.QueuedMsg{Method: "commit", Payload: core.Enc(map[string]any{"payment_id": id, "amount": p.Amount})}
	case "seal":
		return &core.QueuedMsg{Method: "seal", Payload: core.Enc(map[string]any{"payment_id": id})}
	case "finalize":
		return &core.QueuedMsg{Method: "finalize", Payload: core.Enc(map[string]any{"preimage": hex.EncodeToString(p.Preimage)})}
	case "cancel":
		return &core.QueuedMsg{Method: "cancel", Payload: core.Enc(map[string]any{"preimage": hex.EncodeToString(p.Preimage)})}
	case "cleanup":
		return &core.QueuedMsg{Method: "cleanup", Payload: core.Enc(map[string]any{"payment_id": id})}
	case "rollback":
		return &core.QueuedMsg{Method: "rollback", Payload: core.Enc(map[string]any{"payment_id": id})}
	}
	return nil
}

func (e *Engine) enqueue(id string, side int, method string) {
	p := e.Payments[id]
	if p == nil { return }
	core.Enqueue(&p.Side[side].Queue, method)
	if p.Side[side].Account != "" {
		e.kicks = append(e.kicks, p.Side[side].Account)
	}
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

func (e *Engine) ReqCommit(id, peer string, ls *core.Link, ts int64) (bool, error) {
	pp := e.Paths[id]
	if pp == nil || !pp.Commit || pp.Peer[0] != peer { return false, nil }
	if e.Payments[id] != nil { return false, nil }
	delete(e.Paths, id)

	inFee := pp.Fee * e.feeHops(uint64(pp.Hops+1))
	inPen := pp.Penalty * uint64(pp.Hops+1)

	p := &Payment{ID: id, Amount: pp.Amount, Counterpart: pp.Counterpart}
	if len(pp.Preimage) > 0 { p.Preimage = pp.Preimage }
	p.Side[core.IN] = PaymentSide{Account: peer, Fee: inFee, Penalty: inPen, CommitTime: ts}
	if pp.Peer[1] != "" {
		outFee := pp.Fee * e.feeHops(uint64(pp.Hops))
		outPen := pp.Penalty * uint64(pp.Hops)
		p.Side[core.OUT] = PaymentSide{Account: pp.Peer[1], Fee: outFee, Penalty: outPen}
	}
	e.Payments[id] = p

	if pp.Peer[1] != "" {
		core.Enqueue(&p.Side[core.OUT].Queue, "commit"); e.kicks = append(e.kicks, p.Side[core.OUT].Account)
	}
	if pp.Counterpart != "" && pp.Direction == 1 {
		e.deferSendFn(pp.Counterpart, "counterpart_seal", core.Enc(map[string]any{"payment_id": id}))
	}
	e.scheduleCleanup(id, peer, pp.Amount, ts)
	return true, nil
}

func (e *Engine) RespCommit(id string, ls *core.Link, ok bool, ts int64) {
	p := e.Payments[id]
	if p == nil { return }
	core.Dequeue(&p.Side[core.OUT].Queue)
	if ok {
		p.Side[core.OUT].CommitTime = ts
		e.scheduleCleanup(id, p.Side[core.OUT].Account, p.Amount, ts)
	} else {
		if p.Side[core.IN].Account != "" {
			p.Side[core.OUT].Account = ""
			core.Enqueue(&p.Side[core.IN].Queue, "rollback"); e.kicks = append(e.kicks, p.Side[core.IN].Account)
		} else {
			delete(e.Payments, id)
		}
	}
}

// ---------------------------------------------------------------------------
// Seal
// ---------------------------------------------------------------------------

func (e *Engine) ReqSeal(id, peer string, ts int64) (bool, error) {
	p := e.Payments[id]
	if p == nil { return false, fmt.Errorf("seal: unknown payment %s", id) }
	if p.Side[core.IN].Account != peer { return false, fmt.Errorf("seal: wrong peer %s", peer) }
	if p.Side[core.IN].SealTime > 0 { return false, fmt.Errorf("seal: already sealed") }
	p.Side[core.IN].SealTime = ts

	if penaltyElapsed(p.Amount, p.Side[core.IN].CommitTime, ts) >= p.Amount {
		e.enqueue(id, core.IN, "cleanup")
		return true, nil
	}
	if p.Side[core.OUT].Account != "" {
		core.Enqueue(&p.Side[core.OUT].Queue, "seal"); e.kicks = append(e.kicks, p.Side[core.OUT].Account)
	}
	if p.Preimage != nil && p.Side[core.OUT].Account == "" {
		core.Enqueue(&p.Side[core.IN].Queue, "finalize"); e.kicks = append(e.kicks, p.Side[core.IN].Account)
	}
	return true, nil
}

func (e *Engine) RespSeal(id string, ok bool, ts int64) {
	p := e.Payments[id]
	if p == nil { return }
	core.Dequeue(&p.Side[core.OUT].Queue)
	if ok {
		p.Side[core.OUT].SealTime = ts
	}
}

// ---------------------------------------------------------------------------
// Finalize
// ---------------------------------------------------------------------------

func (e *Engine) ReqFinalize(id, peer string, ls *core.Link, ts int64, sellerPreimage []byte) (bool, error) {
	p := e.Payments[id]
	if p == nil { return false, fmt.Errorf("finalize: unknown payment %s", id) }
	if p.Side[core.OUT].Account != peer { return false, fmt.Errorf("finalize: wrong peer %s", peer) }
	if p.Side[core.OUT].SealTime == 0 { return false, fmt.Errorf("finalize: not sealed") }
	side := core.OUT

	ticker := penaltyElapsed(p.Amount, p.Side[side].CommitTime, ts)
	ls.AddBalance(finalizeAmt(p, ticker, side), ts)

	if p.Preimage == nil { p.Preimage = sellerPreimage }
	if p.Side[core.IN].Account != "" {
		core.Enqueue(&p.Side[core.IN].Queue, "finalize"); e.kicks = append(e.kicks, p.Side[core.IN].Account)
	}
	if p.Counterpart != "" && p.Side[core.IN].Account == "" && e.receiptFn != nil {
		e.receiptFn(id, p.Counterpart, e.module, -int64(p.Amount), true)
	}
	p.Side[core.OUT].Account = ""
	if p.Side[core.IN].Account == "" { delete(e.Payments, id) }
	return true, nil
}

func (e *Engine) RespFinalize(id string, ls *core.Link, ok bool, ts int64) {
	p := e.Payments[id]
	if p == nil { return }
	core.Dequeue(&p.Side[core.IN].Queue)

	ticker := penaltyElapsed(p.Amount, p.Side[core.IN].CommitTime, ts)
	ls.AddBalance(finalizeAmt(p, ticker, core.IN), ts)

	if p.Counterpart != "" && e.receiptFn != nil {
		e.receiptFn(id, p.Counterpart, e.module, int64(p.Amount), true)
	}
	p.Side[core.IN].Account = ""
	if p.Side[core.OUT].Account == "" { delete(e.Payments, id) }
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

func (e *Engine) ReqCancel(id, peer string, ls *core.Link, ts int64, cancelPreimage []byte) (bool, error) {
	p := e.Payments[id]
	if p == nil { return false, fmt.Errorf("cancel: unknown payment %s", id) }
	if p.Side[core.IN].Account != peer { return false, fmt.Errorf("cancel: wrong peer %s", peer) }
	if p.Side[core.IN].SealTime > 0 { return false, fmt.Errorf("cancel: already sealed") }

	ls.AddBalance(cancelAmt(p, ts, core.IN), ts)

	if p.Preimage == nil { p.Preimage = cancelPreimage }
	if p.Side[core.OUT].Account != "" { e.enqueue(id, core.OUT, "cancel") }
	if p.Counterpart != "" && p.Side[core.OUT].Account == "" && e.receiptFn != nil {
		e.receiptFn(id, p.Counterpart, e.module, int64(p.Amount), false)
	}
	p.Side[core.IN].Account = ""
	if p.Side[core.OUT].Account == "" { delete(e.Payments, id) }
	return true, nil
}

func (e *Engine) RespCancel(id string, ls *core.Link, ok bool, ts int64) {
	p := e.Payments[id]
	if p == nil { return }
	core.Dequeue(&p.Side[core.OUT].Queue)
	ls.AddBalance(cancelAmt(p, ts, core.OUT), ts)
	if p.Counterpart != "" && p.Side[core.IN].Account == "" && e.receiptFn != nil {
		e.receiptFn(id, p.Counterpart, e.module, -int64(p.Amount), false)
	}
	p.Side[core.OUT].Account = ""
	if p.Side[core.IN].Account == "" { delete(e.Payments, id) }
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

func (e *Engine) ReqCleanup(id, peer string, ls *core.Link, ts int64) (bool, error) {
	p := e.Payments[id]
	if p == nil { return false, fmt.Errorf("cleanup: unknown payment %s", id) }

	// Find the side this peer owns.
	peerSide := -1
	for si := 0; si < 2; si++ {
		if p.Side[si].Account == peer { peerSide = si; break }
	}
	if peerSide < 0 { return false, fmt.Errorf("cleanup: wrong peer %s", peer) }
	if p.Side[peerSide].CommitTime == 0 { return false, fmt.Errorf("cleanup: not committed on side %d", peerSide) }

	// Penalty timer must have expired.
	if penaltyElapsed(p.Amount, p.Side[peerSide].CommitTime, ts) < p.Amount { return false, nil }

	ls.AddBalance(cleanupAmt(p, peerSide), ts)
	p.Side[peerSide].Account = ""
	side := peerSide

	if p.Counterpart != "" && e.receiptFn != nil {
		if side == core.IN && p.Side[core.OUT].Account == "" {
			e.receiptFn(id, p.Counterpart, e.module, int64(p.Amount), false)
		} else if side == core.OUT && p.Side[core.IN].Account == "" {
			e.receiptFn(id, p.Counterpart, e.module, -int64(p.Amount), false)
		}
	}
	if p.Side[0].Account == "" && p.Side[1].Account == "" { delete(e.Payments, id) }
	return true, nil
}

func (e *Engine) RespCleanup(id, peer string, ls *core.Link, ok bool, ts int64) {
	p := e.Payments[id]
	if p == nil { return }
	for si := 0; si < 2; si++ {
		if p.Side[si].Account == peer { core.Dequeue(&p.Side[si].Queue) }
	}
	if !ok {
		e.scheduleFn(RetryDelay, func() {
			p := e.Payments[id]
			if p == nil { return }
			for si := 0; si < 2; si++ {
				if p.Side[si].Account == peer { e.enqueue(id, si, "cleanup"); return }
			}
		})
		return
	}
	for si := 0; si < 2; si++ {
		if p.Side[si].Account == peer {
			ls.AddBalance(cleanupAmt(p, si), ts)
			p.Side[si].Account = ""
		}
	}
	if p.Counterpart != "" && p.Side[0].Account == "" && p.Side[1].Account == "" && e.receiptFn != nil {
		e.receiptFn(id, p.Counterpart, e.module, -int64(p.Amount), false)
	}
	if p.Side[0].Account == "" && p.Side[1].Account == "" { delete(e.Payments, id) }
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

func (e *Engine) ReqRollback(id, peer string) (bool, error) {
	p := e.Payments[id]
	if p == nil { return false, fmt.Errorf("rollback: unknown payment %s", id) }
	if p.Side[core.OUT].Account != "" && p.Side[core.OUT].Account != peer { return false, fmt.Errorf("rollback: wrong peer %s", peer) }
	p.Side[core.OUT].Account = ""
	if p.Side[core.IN].Account != "" {
		core.Enqueue(&p.Side[core.IN].Queue, "rollback"); e.kicks = append(e.kicks, p.Side[core.IN].Account)
	} else {
		if p.Counterpart != "" && e.receiptFn != nil {
			e.receiptFn(id, p.Counterpart, e.module, int64(p.Amount), false)
		}
		delete(e.Payments, id)
	}
	return true, nil
}

func (e *Engine) RespRollback(id string, ok bool) {
	p := e.Payments[id]
	if p == nil { return }
	core.Dequeue(&p.Side[core.IN].Queue)
	p.Side[core.IN].Account = ""
	if p.Side[core.OUT].Account == "" {
		if p.Counterpart != "" && e.receiptFn != nil {
			e.receiptFn(id, p.Counterpart, e.module, -int64(p.Amount), false)
		}
		delete(e.Payments, id)
	}
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

func (e *Engine) scheduleCleanup(id, peer string, amount uint64, commitTime int64) {
	e.doScheduleCleanup(id, peer, penaltyDuration(amount))
}

func (e *Engine) doScheduleCleanup(id, peer string, delay time.Duration) {
	e.scheduleFn(delay, func() {
		p := e.Payments[id]
		if p == nil { return }
		for si := 0; si < 2; si++ {
			if p.Side[si].Account == peer && p.Side[si].CommitTime > 0 {
				e.enqueue(id, si, "cleanup")
				return
			}
		}
	})
}

func (e *Engine) scheduleCancel(id string, delay time.Duration) {
	e.scheduleFn(delay, func() {
		p := e.Payments[id]
		if p == nil || p.Counterpart == "" || p.Side[core.IN].Account != "" { return }
		if p.Side[core.OUT].Account == "" || p.Side[core.OUT].SealTime > 0 { return }
		if p.Preimage == nil { return }
		if core.QueueHas(p.Side[core.OUT].Queue, "seal") { return }
		e.enqueue(id, core.OUT, "cancel")
	})
}

func (e *Engine) ScheduleTimers() {
	now := time.Now().Unix()
	for id, p := range e.Payments {
		for si := 0; si < 2; si++ {
			if p.Side[si].Account != "" && p.Side[si].CommitTime > 0 &&
				!core.QueueHas(p.Side[si].Queue, "cleanup") {
				el := time.Duration(now-p.Side[si].CommitTime) * time.Second
				remaining := penaltyDuration(p.Amount) - el
				if remaining < 0 { remaining = 0 }
				e.doScheduleCleanup(id, p.Side[si].Account, remaining)
			}
		}
		if p.Counterpart != "" && p.Side[core.IN].Account == "" &&
			p.Preimage != nil && p.Side[core.OUT].Account != "" &&
			p.Side[core.OUT].CommitTime > 0 && p.Side[core.OUT].SealTime == 0 &&
			!core.QueueHas(p.Side[core.OUT].Queue, "cancel") {
			remaining := time.Duration(CommitTimeout-(now-p.Side[core.OUT].CommitTime)) * time.Second
			if remaining < 0 { remaining = 0 }
			e.scheduleCancel(id, remaining)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func DecodeHex(s string) ([]byte, error) { return hex.DecodeString(s) }
func HashHex(data []byte) string         { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func DoubleHashHex(data []byte) string   { h1 := sha256.Sum256(data); h2 := sha256.Sum256(h1[:]); return hex.EncodeToString(h2[:]) }
