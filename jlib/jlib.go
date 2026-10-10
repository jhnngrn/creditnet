// Package jlib: shared pathfinder + settlement engine.
// One struct, two files: pathfinder.go (search) and settlement.go (3PC).
package jlib

import (
	"time"

	"creditnet/core"
)

const CommitTimeout = 60 // shared timeout for prepare and cancel phases

type Engine struct {
	// Path state
	Paths       map[string]*Path       `json:"-"`
	PendingReqs map[string]*PendingReq `json:"-"`

	// Settlement state
	Payments    map[string]*Payment    `json:"payments"`

	// Config
	Linear          bool                   // true = climb (CC), false = flood (Ripple)
	SellerFee       bool                   // true = seller gets fee (Resilience)
	GasCharge       uint64
	FeeRate         uint64
	PenaltyRate     uint64
	MaxPerLink      uint8
	MaxDepth        uint8
	RefractoryDelay time.Duration

	// Topology callbacks
	PeersFn     func() []string          `json:"-"`
	ParentFn    func() string            `json:"-"`
	TransferFn  core.TransferFunc        `json:"-"`
	GasPerRound func(depth uint8) uint64 `json:"-"`

	// Core callbacks
	nodeID       string
	module       string
	sendFn       core.SendFunc
	deferSendFn  core.DeferSendFunc
	scheduleFn   core.ScheduleFunc
	bwFn         core.BandwidthFunc
	receiptFn    core.ReceiptFunc
	kicks        []string
}

func NewEngine(nodeID, module string, send core.SendFunc, deferSend core.DeferSendFunc, schedule core.ScheduleFunc, bw core.BandwidthFunc, receipt core.ReceiptFunc) *Engine {
	return &Engine{
		nodeID:      nodeID,
		module:      module,
		Paths:       map[string]*Path{},
		PendingReqs: map[string]*PendingReq{},
		Payments:    map[string]*Payment{},
		sendFn:      send,
		deferSendFn: deferSend,
		scheduleFn:  schedule,
		bwFn:        bw,
		receiptFn:   receipt,
		GasCharge:   1,
		MaxPerLink:  16,
	}
}

func (e *Engine) Bind(nodeID, module string, send core.SendFunc, deferSend core.DeferSendFunc, schedule core.ScheduleFunc, bw core.BandwidthFunc, receipt core.ReceiptFunc) {
	e.nodeID = nodeID
	e.module = module
	e.sendFn = send
	e.deferSendFn = deferSend
	e.scheduleFn = schedule
	e.bwFn = bw
	e.receiptFn = receipt
	if e.Paths == nil { e.Paths = map[string]*Path{} }
	if e.PendingReqs == nil { e.PendingReqs = map[string]*PendingReq{} }
	if e.Payments == nil { e.Payments = map[string]*Payment{} }
}

func (e *Engine) DrainKicks() []string { k := e.kicks; e.kicks = nil; return k }
func (e *Engine) Module() string       { return e.module }

func (e *Engine) feeHops(h uint64) uint64 {
	if e.SellerFee { return h }
	if h == 0 { return 0 }
	return h - 1
}

func (e *Engine) OnRemoveLink(peer string) {
	for id, p := range e.Paths {
		if p.Peer[0] == peer || p.Peer[1] == peer { delete(e.Paths, id) }
	}
	for id, p := range e.Payments {
		for si := 0; si < 2; si++ {
			if p.Side[si].Account == peer { p.Side[si] = PaymentSide{} }
		}
		if p.Side[0].Account == "" && p.Side[1].Account == "" { delete(e.Payments, id) }
	}
}

func (e *Engine) Reserved(peer string, side int) uint64 {
	var total uint64
	// Committed paths
	for _, p := range e.Paths {
		if !p.Commit { continue }
		if side == core.IN && p.Peer[0] == peer {
			h := uint64(p.Hops + 1)
			total += p.Amount + p.Fee*e.feeHops(h) + p.Penalty*h
		}
		if side == core.OUT && p.Peer[1] == peer {
			h := uint64(p.Hops)
			total += p.Amount + p.Fee*e.feeHops(h) + p.Penalty*h
		}
	}
	// Active payments
	for _, p := range e.Payments {
		s := &p.Side[side]
		if s.Account != peer { continue }
		total += p.Amount + s.Fee + s.Penalty
	}
	return total
}

func SyncMethods() []string {
	return []string{"commit", "seal", "finalize", "cancel", "cleanup", "rollback"}
}

func MsgMethods() []string {
	return []string{"search", "recurse", "found", "prepare"}
}

func OpenMsgMethods() []string {
	return []string{"payment_request", "payment_accept", "counterpart_search", "counterpart_commit", "counterpart_seal"}
}
