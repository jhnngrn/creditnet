package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Bandwidth — core computes, modules report Reserved
// ---------------------------------------------------------------------------

// pendingBalance returns the effective debt on a link after all reservations.
// Positive means the peer owes us (side=OUT) or we owe them (side=IN).
func (n *node) pendingBalance(peer string, side int) int64 {
	l := n.links[peer]
	if l == nil {
		return 0
	}
	bal := l.EffectiveBalance(time.Now().Unix())
	var debt int64
	if side == OUT {
		debt = bal
	} else {
		debt = -bal
	}
	for _, mod := range n.modules {
		debt -= int64(mod.Reserved(peer, side))
	}
	return debt
}

// bw = credit_limit + pendingBalance, floored at 0.
func (n *node) bw(peer string, side int) uint64 {
	l := n.links[peer]
	if l == nil {
		return 0
	}
	var limit int64
	if side == OUT {
		limit = int64(l.CreditLimitIn)
	} else {
		limit = int64(l.CreditLimit)
	}
	// pendingBalance already uses EffectiveBalance
	total := limit + n.pendingBalance(peer, side)
	if total < 0 {
		return 0
	}
	return uint64(total)
}

func (n *node) bwFunc() BandwidthFunc             { return n.bw }
func (n *node) pbFunc() PendingBalanceFunc         { return n.pendingBalance }


// ---------------------------------------------------------------------------
// Receipts
// ---------------------------------------------------------------------------

func (n *node) addReceipt(id, peer, module string, amount int64, ok bool) {
	if len(n.receipts) >= MaxReceipts {
		n.receipts = n.receipts[1:]
	}
	n.receipts = append(n.receipts, Receipt{
		ID: id, Peer: peer, Module: module, Amount: amount, Status: ok, Timestamp: time.Now().Unix(),
	})
}

func (n *node) receiptFunc() ReceiptFunc { return n.addReceipt }
func (n *node) transferFunc() TransferFunc {
	return func(peer string, amount uint64) { n.transferOutbox[peer] += amount }
}

// ---------------------------------------------------------------------------
// Core-owned sync handlers: sync_trustline, payment
// ---------------------------------------------------------------------------

func (n *node) handleCoreSyncRequest(peer string, ls *Link, method string, payload json.RawMessage, ts int64) (bool, error) {
	switch method {
	case "sync_trustline":
		var m struct{ Amount uint64 }
		if json.Unmarshal(payload, &m) != nil {
			return false, fmt.Errorf("bad payload")
		}
		ls.CreditLimitIn = m.Amount
		return true, nil

	case "payment":
		var m struct{ Amount uint64 }
		if json.Unmarshal(payload, &m) != nil {
			return false, fmt.Errorf("bad payload")
		}
		if m.Amount == 0 || n.bw(peer, IN) < m.Amount {
			return false, nil
		}
		ls.AddBalance(int64(m.Amount), ts)
		n.addReceipt("", peer, "", int64(m.Amount), true)
		return true, nil

	case "sync_demurrage":
		var m struct{ Rate uint64 }
		if json.Unmarshal(payload, &m) != nil {
			return false, fmt.Errorf("bad payload")
		}
		ls.MaterializeDemurrage(ts)
		ls.DemurrageIn = m.Rate
		return true, nil

	case "transfer":
		var m struct{ Amount uint64 }
		if json.Unmarshal(payload, &m) != nil {
			return false, fmt.Errorf("bad payload")
		}
		ls.AddBalance(int64(m.Amount), ts)
		return true, nil

	case "sync_module":
		var m struct {
			Module string `json:"module"`
			Active bool   `json:"active"`
		}
		if json.Unmarshal(payload, &m) != nil {
			return false, fmt.Errorf("bad payload")
		}
		if m.Active {
			// Peer wants to activate — check if we have and accept this module.
			found := false
			for _, r := range n.srv.regs {
				if r.name == m.Module { found = true; break }
			}
			if !found { return false, nil }
			_, exists := ls.Modules[m.Module]
			if !exists { return false, nil } // not enabled on our side
			ls.Modules[m.Module] = true
			// Clean outbox — confirmed by peer, no need to propose.
			if mods := n.moduleOutbox[peer]; mods != nil {
				delete(mods, m.Module)
				if len(mods) == 0 { delete(n.moduleOutbox, peer) }
			}
			return true, nil
		}
		// Deactivation: peer is removing this module. Keep false to preserve our intent.
		if _, exists := ls.Modules[m.Module]; exists {
			ls.Modules[m.Module] = false
		}
		return true, nil
	}
	return false, nil
}

func (n *node) handleCoreSyncResponse(peer string, ls *Link, method string, payload json.RawMessage, ok bool, ts int64) {
	switch method {
	case "sync_trustline":
		var m struct{ Amount uint64 }
		json.Unmarshal(payload, &m)
		if limit, exists := n.limitOutbox[peer]; exists && limit == m.Amount {
			delete(n.limitOutbox, peer)
		}
		if ok {
			ls.CreditLimit = m.Amount
		}

	case "payment":
		var m struct{ Amount uint64 }
		json.Unmarshal(payload, &m)
		delete(n.directOutbox, peer)
		if ok {
			ls.AddBalance(-int64(m.Amount), ts)
			n.addReceipt("", peer, "", -int64(m.Amount), true)
		} else {
			n.addReceipt("", peer, "", -int64(m.Amount), false)
		}

	case "sync_demurrage":
		var m struct{ Rate uint64 }
		json.Unmarshal(payload, &m)
		if rate, exists := n.demurrageOutbox[peer]; exists && rate == m.Rate {
			delete(n.demurrageOutbox, peer)
		}
		if ok {
			ls.MaterializeDemurrage(ts)
			ls.Demurrage = m.Rate
		}

	case "transfer":
		var m struct{ Amount uint64 }
		json.Unmarshal(payload, &m)
		if n.transferOutbox[peer] >= m.Amount {
			n.transferOutbox[peer] -= m.Amount
		} else {
			n.transferOutbox[peer] = 0
		}
		if n.transferOutbox[peer] == 0 { delete(n.transferOutbox, peer) }
		ls.AddBalance(-int64(m.Amount), ts)

	case "sync_module":
		var m struct {
			Module string `json:"module"`
			Active bool   `json:"active"`
		}
		json.Unmarshal(payload, &m)
		// Clean outbox — one-shot, don't re-propose.
		if mods := n.moduleOutbox[peer]; mods != nil {
			delete(mods, m.Module)
			if len(mods) == 0 { delete(n.moduleOutbox, peer) }
		}
		if m.Active && ok {
			ls.Modules[m.Module] = true
		}
		if !m.Active {
			delete(ls.Modules, m.Module)
		}
		// NACK on activation: false stays in Modules — peer can still propose to us.
	}
}

// ---------------------------------------------------------------------------
// Core-owned message handler: link_request
// ---------------------------------------------------------------------------

const maxPendingLinkRequests = 64

func (n *node) handleCoreMsgRequest(sender, method string) {
	if method == "link_request" {
		if n.links[sender] == nil && len(n.linkRequests) < maxPendingLinkRequests {
			n.linkRequests[sender] = time.Now().Unix()
		}
	}
}

// ---------------------------------------------------------------------------
// Promote: core outboxes first, then modules, then gas
// ---------------------------------------------------------------------------

func (n *node) nextTx(peer string) *QueuedMsg {
	// Core outboxes first — rare, one-shot, get them out of the way.
	if amount, ok := n.directOutbox[peer]; ok {
		return &QueuedMsg{Method: "payment", Payload: Enc(map[string]uint64{"amount": amount})}
	}
	if limit, ok := n.limitOutbox[peer]; ok {
		return &QueuedMsg{Method: "sync_trustline", Payload: Enc(map[string]uint64{"amount": limit})}
	}
	if rate, ok := n.demurrageOutbox[peer]; ok {
		return &QueuedMsg{Method: "sync_demurrage", Payload: Enc(map[string]uint64{"rate": rate})}
	}
	link := n.links[peer]

	// Module activation/deactivation — one-shot proposals.
	if mods, ok := n.moduleOutbox[peer]; ok {
		for mod, activate := range mods {
			return &QueuedMsg{Method: "sync_module", Payload: Enc(map[string]any{"module": mod, "active": activate})}
		}
	}

	// Module operations — prefix method with module name. Skip modules not enabled for this link.
	for i, mod := range n.modules {
		if link != nil && !link.ModuleEnabled(n.srv.regs[i].name) { continue }
		if msg := mod.NextTx(peer); msg != nil {
			msg.Method = n.srv.regs[i].name + "." + msg.Method
			return msg
		}
	}

	// Internal transfer last — background compensation, lowest priority.
	if amount, ok := n.transferOutbox[peer]; ok && amount >= TransferThreshold {
		return &QueuedMsg{Method: "transfer", Payload: Enc(map[string]uint64{"amount": amount})}
	}
	return nil
}

func (n *node) drainAllKicks() []string {
	var all []string
	for _, mod := range n.modules {
		all = append(all, mod.DrainKicks()...)
	}
	return all
}

func (n *node) deferSendFunc() DeferSendFunc {
	return func(to, method string, payload json.RawMessage) {
		n.deferredMsgs = append(n.deferredMsgs, Msg{To: to, Method: method, Payload: payload})
	}
}

func (n *node) drainDeferredMsgs() []Msg {
	m := n.deferredMsgs; n.deferredMsgs = nil; return m
}
