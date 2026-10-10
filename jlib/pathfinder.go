// Path search: flood (Ripple) or climb (CC), payment negotiation, found/prepare.
package jlib

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"time"

	"creditnet/core"
)

// validateRate reports whether value/amount >= rate/2000 using 128-bit arithmetic.
func validateRate(value, amount, rate uint64) bool {
	hi1, lo1 := bits.Mul64(value, 2000)
	hi2, lo2 := bits.Mul64(amount, rate)
	return hi1 > hi2 || (hi1 == hi2 && lo1 >= lo2)
}

// OverflowCheck returns amount + fee*feeHops + penalty*hops, or false on overflow.
func (e *Engine) OverflowCheck(amount, fee, penalty, hops uint64) (uint64, bool) {
	hi1, fh := bits.Mul64(fee, e.feeHops(hops))
	hi2, ph := bits.Mul64(penalty, hops)
	if hi1 > 0 || hi2 > 0 { return 0, false }
	sum, c1 := bits.Add64(amount, fh, 0)
	sum, c2 := bits.Add64(sum, ph, 0)
	if c1 > 0 || c2 > 0 || sum > uint64(math.MaxInt64) { return 0, false }
	return sum, true
}

const (
	SearchTTL       = 30  // renewed on each search (gas is the rent)
	EndpointPathTTL = 300 // 5 min
	RequestTTL      = 900 // 15 min
)

type Path struct {
	Peer        [2]string
	Counterpart string
	Amount      uint64
	Fee         uint64
	Penalty     uint64
	Gas         uint64
	Preimage    []byte
	Direction   uint8
	Hops        uint8
	Depth       uint8
	DepthAck    bool
	Commit      bool
	timer       *time.Timer
}

type PendingReq struct {
	Peer      string `json:"peer"`
	Amount    uint64 `json:"amount"`
	Fee       uint64 `json:"fee"`
	Penalty   uint64 `json:"penalty"`
	Direction uint8  `json:"direction"`
	ExpiresAt int64  `json:"expires_at"`
}

type SearchMsg struct {
	ID        string `json:"payment_id"`
	Direction uint8  `json:"direction"`
	Gas       uint64 `json:"gas"`
	Amount    uint64 `json:"amount"`
	Fee       uint64 `json:"fee"`
	Penalty   uint64 `json:"penalty"`
	Hops      uint8  `json:"hops"`
}

// linkSlots counts operations on a link.
// reserved=false: all paths + all payments (admission).
// reserved=true: committed paths + all payments (bandwidth).
func (e *Engine) linkSlots(peer string, reserved bool) uint8 {
	var n uint8
	for _, p := range e.Paths {
		if p.Peer[0] != peer && p.Peer[1] != peer { continue }
		if reserved && !p.Commit { continue }
		n++
	}
	for _, pay := range e.Payments {
		if pay.Side[0].Account == peer || pay.Side[1].Account == peer { n++ }
	}
	return n
}

// ---------------------------------------------------------------------------
// Payment request/accept
// ---------------------------------------------------------------------------

func (e *Engine) MsgPaymentRequest(sender string, raw json.RawMessage) {
	var m struct {
		TempID  string `json:"temp_id"`
		Buyer   string `json:"buyer"`
		Amount  uint64 `json:"amount"`
		Fee     uint64 `json:"fee"`
		Penalty uint64 `json:"penalty"`
	}
	if json.Unmarshal(raw, &m) != nil || m.Amount == 0 { return }
	if len(e.PendingReqs) >= 100 { return }
	e.PendingReqs[m.TempID] = &PendingReq{Peer: m.Buyer, Amount: m.Amount, Fee: m.Fee, Penalty: m.Penalty, Direction: 1, ExpiresAt: time.Now().Unix() + RequestTTL}
	tid := m.TempID
	e.scheduleFn(time.Duration(RequestTTL)*time.Second, func() { delete(e.PendingReqs, tid) })
}

func (e *Engine) AcceptPayment(tempID string, send core.SendFunc) (string, error) {
	pr := e.PendingReqs[tempID]
	if pr == nil || pr.Direction != 1 { return "", fmt.Errorf("unknown or invalid request") }
	delete(e.PendingReqs, tempID)

	sellerPre := make([]byte, 32)
	rand.Read(sellerPre)
	h1 := sha256.Sum256(sellerPre)
	cancelPre := h1[:]
	h2 := sha256.Sum256(cancelPre)
	pid := hex.EncodeToString(h2[:])

	p := &Path{Amount: pr.Amount, Fee: pr.Fee, Penalty: pr.Penalty, Preimage: sellerPre, Counterpart: pr.Peer, Direction: 1}
	e.Paths[pid] = p
	e.schedulePathExpiry(pid, EndpointPathTTL)

	send(pr.Peer, "payment_accept", core.Enc(map[string]any{
		"temp_id": tempID, "cancel_preimage": hex.EncodeToString(cancelPre),
	}))
	return pid, nil
}

func (e *Engine) SendPayment(recipient string, amount uint64, send core.SendFunc) string {
	if recipient == "" || amount == 0 { return "" }
	tmp := make([]byte, 16)
	rand.Read(tmp)
	tempID := hex.EncodeToString(tmp)

	fee := amount * e.FeeRate / 1000
	pen := amount * e.PenaltyRate / 1000

	e.PendingReqs[tempID] = &PendingReq{Peer: recipient, Amount: amount, Fee: fee, Penalty: pen, Direction: 0, ExpiresAt: time.Now().Unix() + RequestTTL}
	tid := tempID
	e.scheduleFn(time.Duration(RequestTTL)*time.Second, func() { delete(e.PendingReqs, tid) })

	send(recipient, "payment_request", core.Enc(map[string]any{
		"temp_id": tempID, "buyer": e.nodeID, "amount": amount, "fee": fee, "penalty": pen,
	}))
	return tempID
}

func (e *Engine) MsgPaymentAccept(sender string, raw json.RawMessage) {
	var m struct {
		TempID         string `json:"temp_id"`
		CancelPreimage string `json:"cancel_preimage"`
	}
	if json.Unmarshal(raw, &m) != nil { return }

	pr := e.PendingReqs[m.TempID]
	if pr == nil || pr.Direction != 0 || pr.Peer != sender { return }
	delete(e.PendingReqs, m.TempID)

	pre, _ := hex.DecodeString(m.CancelPreimage)
	h := sha256.Sum256(pre)
	pid := hex.EncodeToString(h[:])

	p := &Path{Amount: pr.Amount, Fee: pr.Fee, Penalty: pr.Penalty, Preimage: pre, Counterpart: sender, Direction: 0}
	e.Paths[pid] = p
	e.schedulePathExpiry(pid, EndpointPathTTL)

	// Buyer starts first search round.
	if e.GasPerRound != nil { p.Gas += e.GasPerRound(0) }
	e.doSearch(pid, p, p.Direction)
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

func (e *Engine) MsgSearch(sender string, raw json.RawMessage) {
	var m SearchMsg
	if json.Unmarshal(raw, &m) != nil { return }
	dir := m.Direction
	if dir > 1 { return }

	p := e.Paths[m.ID]

	// First visit: validate, register, recurse.
	if p == nil {
		if m.Amount == 0 { return }
		total, ok := e.OverflowCheck(m.Amount, m.Fee, m.Penalty, 1)
		if !ok { return }
		if e.FeeRate > 0 && !validateRate(m.Fee, m.Amount, e.FeeRate) { return }
		if e.PenaltyRate > 0 && !validateRate(m.Penalty, m.Amount, e.PenaltyRate) { return }
		if e.bwFn(sender, int(dir)) < total { return }
		if e.linkSlots(sender, false) >= e.MaxPerLink { return }

		p = &Path{Amount: m.Amount, Fee: m.Fee, Penalty: m.Penalty, Hops: m.Hops}
		e.Paths[m.ID] = p
		e.schedulePathExpiry(m.ID, SearchTTL)
		p.Peer[dir] = sender
		if m.Gas >= e.GasCharge {
			p.Gas += m.Gas - e.GasCharge
		}
		e.sendRecurse(sender, m.ID, dir, 0)
		return
	}

	// Already matched or committed.
	if (p.Peer[0] != "" && p.Peer[1] != "") || p.Commit { return }

	// Re-visit: charge, forward.
	if p.Peer[dir] != "" {
		if sender != p.Peer[dir] { return }
		if p.Counterpart != "" { return }
		if m.Gas < e.GasCharge { return }
		p.Gas += m.Gas - e.GasCharge
		e.schedulePathExpiry(m.ID, SearchTTL)
		e.doSearch(m.ID, p, dir)
		return
	}

	// Intermediary collision.
	if p.Counterpart == "" && p.Peer[1-dir] != "" {
		if p.Peer[1-dir] == sender { return }
		if dir == 1 {
			p.Peer[1] = sender
			p.Hops = m.Hops
			e.sendFn(p.Peer[0], "found", core.Enc(map[string]any{"payment_id": m.ID, "hops": p.Hops}))
		} else {
			e.sendFn(sender, "found", core.Enc(map[string]any{"payment_id": m.ID, "hops": p.Hops}))
		}
		return
	}

	// Endpoint collision.
	if p.Counterpart != "" && dir != p.Direction {
		if p.Direction == 0 {
			if m.Hops > uint8(p.Depth)*2+1 { return }
			p.Peer[dir] = sender
			p.Hops = m.Hops
			e.sendFn(sender, "prepare", core.Enc(map[string]any{
				"payment_id": m.ID,
			}))
		} else {
			e.sendFn(sender, "found", core.Enc(map[string]any{"payment_id": m.ID, "hops": uint8(0)}))
		}
		return
	}
}

// ---------------------------------------------------------------------------
// Search propagation — flood or climb
// ---------------------------------------------------------------------------

func (e *Engine) doSearch(id string, p *Path, dir uint8) {
	if e.Linear {
		e.climbSearch(id, p, dir)
	} else {
		e.floodSearch(id, p, dir)
	}
}

func (e *Engine) floodSearch(id string, p *Path, dir uint8) {
	if p.Gas == 0 { return }
	p.Depth++
	p.DepthAck = false
	peers := e.PeersFn()
	var targets []string
	for _, peer := range peers {
		if peer == p.Peer[dir] { continue }
		targets = append(targets, peer)
	}
	if len(targets) == 0 { return }
	n := uint64(len(targets))
	share := p.Gas / n
	extra := p.Gas % n
	if share < 1 { share = 1; extra = 0 }
	for i, peer := range targets {
		s := share
		if uint64(i) < extra { s++ }
		if p.Gas < s { break }
		e.sendFn(peer, "search", core.Enc(SearchMsg{
			ID: id, Direction: dir, Gas: s,
			Amount: p.Amount, Fee: p.Fee, Penalty: p.Penalty, Hops: p.Hops + 1,
		}))
		if e.TransferFn != nil { e.TransferFn(peer, s) }
		p.Gas -= s
	}
}

func (e *Engine) climbSearch(id string, p *Path, dir uint8) {
	parent := e.ParentFn()
	if parent == "" {
		if p.Peer[dir] != "" {
			e.sendRecurse(p.Peer[dir], id, dir, 0)
		}
		return
	}
	p.Depth++
	if e.TransferFn != nil { e.TransferFn(parent, e.GasCharge) }
	e.sendFn(parent, "search", core.Enc(SearchMsg{
		ID: id, Direction: dir, Gas: p.Gas,
		Amount: p.Amount, Fee: p.Fee, Penalty: p.Penalty, Hops: p.Hops,
	}))
	p.Gas = 0
}

// ---------------------------------------------------------------------------
// Recurse
// ---------------------------------------------------------------------------

func (e *Engine) MsgRecurse(sender string, raw json.RawMessage) {
	var m struct {
		ID        string `json:"payment_id"`
		Direction uint8  `json:"direction"`
		Depth     uint8  `json:"depth"`
	}
	if json.Unmarshal(raw, &m) != nil { return }

	p := e.Paths[m.ID]
	if p == nil || p.Commit { return }
	if p.Peer[0] != "" && p.Peer[1] != "" { return }

	if !e.Linear {
		if p.DepthAck { return }
		if m.Depth+1 != p.Depth { return }
		p.DepthAck = true
	}

	if p.Counterpart != "" {
		e.sendFn(p.Counterpart, "counterpart_search", core.Enc(map[string]any{"payment_id": m.ID}))
		return
	}

	if p.Peer[m.Direction] != "" {
		e.sendRecurse(p.Peer[m.Direction], m.ID, m.Direction, p.Depth)
	}
}

func (e *Engine) sendRecurse(peer, id string, dir uint8, depth uint8) {
	if e.Linear {
		e.sendFn(peer, "recurse", core.Enc(map[string]any{
			"payment_id": id, "direction": dir,
		}))
	} else {
		e.sendFn(peer, "recurse", core.Enc(map[string]any{
			"payment_id": id, "direction": dir, "depth": depth,
		}))
	}
}

// ---------------------------------------------------------------------------
// Counterpart messages
// ---------------------------------------------------------------------------

func (e *Engine) MsgCounterpartSearch(sender string, raw json.RawMessage) {
	var m struct{ ID string `json:"payment_id"` }
	if json.Unmarshal(raw, &m) != nil { return }

	p := e.Paths[m.ID]
	if p == nil || p.Counterpart != sender || p.Commit { return }
	if p.Peer[0] != "" && p.Peer[1] != "" { return }
	if e.MaxDepth > 0 && p.Depth >= e.MaxDepth { return }

	if e.RefractoryDelay > 0 {
		pid := m.ID
		e.scheduleFn(e.RefractoryDelay, func() {
			e.doCounterpartSearch(pid)
		})
	} else {
		e.doCounterpartSearch(m.ID)
	}
}

func (e *Engine) doCounterpartSearch(id string) {
	p := e.Paths[id]
	if p == nil || p.Commit { return }
	if e.MaxDepth > 0 && p.Depth >= e.MaxDepth { return }
	p.DepthAck = false
	if e.GasPerRound != nil { p.Gas += e.GasPerRound(p.Depth) }
	e.doSearch(id, p, p.Direction)
}

func (e *Engine) MsgCounterpartCommit(sender string, raw json.RawMessage) {
	var m struct{ ID string `json:"payment_id"` }
	if json.Unmarshal(raw, &m) != nil { return }

	p := e.Paths[m.ID]
	if p == nil || p.Counterpart != sender || p.Preimage == nil { return }
	if p.Peer[1] == "" { return }

	e.StartPayment(p)
	delete(e.Paths, m.ID)
}

func (e *Engine) MsgCounterpartSeal(sender string, raw json.RawMessage) {
	var m struct{ ID string `json:"payment_id"` }
	if json.Unmarshal(raw, &m) != nil { return }
	op := e.Payments[m.ID]
	if op == nil || op.Counterpart != sender { return }
	if op.Side[core.OUT].SealTime > 0 || core.QueueHas(op.Side[core.OUT].Queue, "seal") { return }
	core.Enqueue(&op.Side[core.OUT].Queue, "seal")
	e.kicks = append(e.kicks, op.Side[core.OUT].Account)
}

// ---------------------------------------------------------------------------
// Found / Prepare
// ---------------------------------------------------------------------------

func (e *Engine) MsgFound(sender string, raw json.RawMessage) {
	var m struct {
		ID   string `json:"payment_id"`
		Hops uint8  `json:"hops"`
	}
	if json.Unmarshal(raw, &m) != nil { return }

	p := e.Paths[m.ID]
	if p == nil || p.Peer[1] != "" { return }
	if p.Direction != 0 { return } // only buyer-direction paths accept found

	if p.Counterpart != "" && p.Direction == 0 {
		if m.Hops+1 > uint8(p.Depth)*2+1 { return }
	}

	p.Peer[1] = sender
	p.Hops = m.Hops + 1

	if p.Counterpart != "" && p.Direction == 0 {
		e.sendFn(sender, "prepare", core.Enc(map[string]any{
			"payment_id": m.ID,
		}))
		return
	}

	if p.Peer[0] != "" {
		e.sendFn(p.Peer[0], "found", core.Enc(map[string]any{"payment_id": m.ID, "hops": p.Hops}))
	}
}

func (e *Engine) MsgPrepare(sender string, raw json.RawMessage) {
	var m struct {
		ID string `json:"payment_id"`
	}
	if json.Unmarshal(raw, &m) != nil { return }

	p := e.Paths[m.ID]
	if p == nil || p.Commit { return }

	if p.Peer[0] != "" && p.Peer[0] != sender { return }

	// Bandwidth checks before peer assignment to avoid poisoning Peer[0].
	inTotal, ok := e.OverflowCheck(p.Amount, p.Fee, p.Penalty, uint64(p.Hops)+1)
	if !ok { return }
	if e.bwFn(sender, core.IN) < inTotal { return }
	if p.Hops > 0 {
		outTotal, ok := e.OverflowCheck(p.Amount, p.Fee, p.Penalty, uint64(p.Hops))
		if !ok { return }
		if p.Peer[1] != "" && e.bwFn(p.Peer[1], core.OUT) < outTotal { return }
	}

	p.Peer[0] = sender
	p.Commit = true
	e.schedulePathExpiry(m.ID, CommitTimeout)

	if p.Counterpart != "" && p.Direction == 1 {
		e.sendFn(p.Counterpart, "counterpart_commit", core.Enc(map[string]any{"payment_id": m.ID}))
		return
	}

	if p.Peer[1] != "" {
		e.sendFn(p.Peer[1], "prepare", core.Enc(m))
	}
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

func (e *Engine) schedulePathExpiry(id string, ttl int) {
	p := e.Paths[id]
	if p == nil { return }
	if p.timer != nil { p.timer.Stop() }
	p.timer = e.scheduleFn(time.Duration(ttl)*time.Second, func() {
		if p := e.Paths[id]; p != nil {
			if p.Counterpart != "" && e.receiptFn != nil {
				amt := int64(p.Amount)
				if p.Direction == 0 { amt = -amt }
				e.receiptFn(id, p.Counterpart, e.module, amt, false)
			}
			delete(e.Paths, id)
		}
	})
}
