package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

var ErrUnknownMethod = errors.New("unknown method")

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	sess, _, err := s.authenticate(r)
	if err != nil { HttpError(w, http.StatusUnauthorized, err.Error()); return }
	req, ok := readAPIRequest(w, r); if !ok { return }
	n := s.getNode(sess.user)
	if n == nil { HttpError(w, http.StatusInternalServerError, "no node"); return }

	n.mu.Lock()
	ops := n.accountOps()

	var result json.RawMessage
	var appErr error
	switch req.Method {
	case "add_link":
		result, appErr = n.accountAddLink(req.Payload, ops)
	case "remove_link":
		result, appErr = n.accountRemoveLink(req.Payload, ops)
	case "set_credit_limit":
		result, appErr = n.accountSetCreditLimit(req.Payload)
	case "status":
		result = n.accountStatus()
	case "pending_links":
		result = n.accountPendingLinks()
	case "payment":
		result, appErr = n.accountDirectPayment(req.Payload)
	case "set_demurrage":
		result, appErr = n.accountSetDemurrage(req.Payload)
	case "dismiss_link":
		result, appErr = n.accountDismissLink(req.Payload)
	case "set_modules":
		result, appErr = n.accountSetModules(req.Payload)
	default:
		if idx, ok := s.acctRoute[req.Method]; ok {
			regName := s.regs[idx].name
			var peerCheck struct{ Peer string `json:"peer"` }
			json.Unmarshal(req.Payload, &peerCheck)
			if p := strings.ToLower(peerCheck.Peer); p != "" {
				if l := n.links[p]; l != nil && !l.ModuleEnabled(regName) {
					appErr = fmt.Errorf("module not active on link to %s", p)
					break
				}
			}
			shortMethod := strings.TrimPrefix(req.Method, regName+".")
			result, appErr = n.modules[idx].HandleAccountRequest(shortMethod, req.Payload, ops)
		} else {
			appErr = ErrUnknownMethod
		}
	}

	kicks := n.drainAllKicks()
	if err := n.commitLocked(); err != nil { n.mu.Unlock(); HttpError(w, http.StatusInternalServerError, "persist failed"); return }
	n.mu.Unlock()
	for _, peer := range kicks { n.kick(peer) }
	if appErr != nil { ApiError(w, appErr.Error()); return }
	if result == nil { result = json.RawMessage("true") }
	ApiOK(w, result)
}

func (n *node) accountAddLink(raw json.RawMessage, ops AccountOps) (json.RawMessage, error) {
	var m struct {
		Peer    string   `json:"peer"`
		Limit   uint64   `json:"credit_limit"`
		Modules []string `json:"modules"`
	}
	if json.Unmarshal(raw, &m) != nil { return nil, fmt.Errorf("bad request") }
	peer := strings.ToLower(m.Peer)
	if err := ops.AddLink(peer); err != nil { return nil, err }
	mods := map[string]bool{}
	if m.Modules == nil {
		// Omitted → all registered modules, pending activation (matches makeNode migration).
		for _, r := range n.srv.regs { mods[r.name] = false }
	} else {
		// Explicit list (possibly empty → core only).
		for _, mod := range m.Modules { mods[mod] = false }
	}
	n.links[peer].Modules = mods
	// Queue activation proposals for all pending modules.
	for mod := range mods {
		if n.moduleOutbox[peer] == nil { n.moduleOutbox[peer] = map[string]bool{} }
		n.moduleOutbox[peer][mod] = true
	}
	delete(n.linkRequests, peer)
	ops.SendMessage(peer, "link_request", nil)
	if m.Limit > 0 { n.limitOutbox[peer] = m.Limit }
	n.kick(peer)
	return Enc("ok"), nil
}

func (n *node) accountRemoveLink(raw json.RawMessage, ops AccountOps) (json.RawMessage, error) {
	var m struct{ Peer string `json:"peer"` }
	if json.Unmarshal(raw, &m) != nil { return nil, fmt.Errorf("bad request") }
	if err := ops.RemoveLink(strings.ToLower(m.Peer)); err != nil { return nil, err }
	return Enc("ok"), nil
}

func (n *node) accountSetCreditLimit(raw json.RawMessage) (json.RawMessage, error) {
	var m struct{ Peer string `json:"peer"`; Limit uint64 `json:"limit"` }
	if json.Unmarshal(raw, &m) != nil { return nil, fmt.Errorf("bad request") }
	peer := strings.ToLower(m.Peer)
	if n.links[peer] == nil { return nil, fmt.Errorf("no link to %s", peer) }
	n.limitOutbox[peer] = m.Limit; n.kick(peer)
	return Enc("ok"), nil
}

func (n *node) accountStatus() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range n.links {
		now := time.Now().Unix()
		info := map[string]any{
			"balance": l.EffectiveBalance(now), "credit_limit": l.CreditLimit, "credit_limit_in": l.CreditLimitIn,
			"bandwidth_in": n.bw(peer, IN), "bandwidth_out": n.bw(peer, OUT),
			"demurrage": l.Demurrage, "demurrage_in": l.DemurrageIn,
		}
		info["modules"] = l.Modules
		links[peer] = info
	}
	status := map[string]any{"links": links, "receipts": n.receipts}
	for i, mod := range n.modules {
		if extra := mod.Status(); extra != nil {
			var m map[string]any
			if json.Unmarshal(extra, &m) == nil { status[n.srv.regs[i].name] = m }
		}
	}
	return Enc(status)
}

func (n *node) accountPendingLinks() json.RawMessage {
	var out []map[string]any
	for peer, ts := range n.linkRequests { out = append(out, map[string]any{"from": peer, "timestamp": ts}) }
	return Enc(out)
}

func (n *node) accountDirectPayment(raw json.RawMessage) (json.RawMessage, error) {
	var m struct{ Peer string `json:"peer"`; Amount uint64 `json:"amount"` }
	if json.Unmarshal(raw, &m) != nil { return nil, fmt.Errorf("bad request") }
	peer := strings.ToLower(m.Peer)
	if n.links[peer] == nil { return nil, fmt.Errorf("no link to %s", peer) }
	if m.Amount == 0 { return nil, fmt.Errorf("amount required") }
	if _, ok := n.directOutbox[peer]; ok { return nil, fmt.Errorf("already pending") }
	n.directOutbox[peer] = m.Amount; n.kick(peer)
	return Enc("ok"), nil
}

func (n *node) accountSetDemurrage(raw json.RawMessage) (json.RawMessage, error) {
	var m struct {
		Peer string `json:"peer"`
		Rate uint64 `json:"rate"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return nil, fmt.Errorf("bad request")
	}
	peer := strings.ToLower(m.Peer)
	if n.links[peer] == nil {
		return nil, fmt.Errorf("no link to %s", peer)
	}
	n.demurrageOutbox[peer] = m.Rate
	n.kick(peer)
	return Enc("ok"), nil
}

func (n *node) accountDismissLink(raw json.RawMessage) (json.RawMessage, error) {
	var m struct{ Peer string `json:"peer"` }
	if json.Unmarshal(raw, &m) != nil { return nil, fmt.Errorf("bad request") }
	delete(n.linkRequests, strings.ToLower(m.Peer))
	return Enc("ok"), nil
}

func (n *node) accountSetModules(raw json.RawMessage) (json.RawMessage, error) {
	var m struct {
		Peer    string   `json:"peer"`
		Modules []string `json:"modules"`
	}
	if json.Unmarshal(raw, &m) != nil { return nil, fmt.Errorf("bad request") }
	peer := strings.ToLower(m.Peer)
	link := n.links[peer]
	if link == nil { return nil, fmt.Errorf("no link to %s", peer) }
	if m.Modules == nil { m.Modules = []string{} }
	desired := map[string]bool{}
	for _, mod := range m.Modules { desired[mod] = true }
	// Add desired modules not already present.
	for mod := range desired {
		if _, exists := link.Modules[mod]; !exists {
			link.Modules[mod] = false
			if n.moduleOutbox[peer] == nil { n.moduleOutbox[peer] = map[string]bool{} }
			n.moduleOutbox[peer][mod] = true
		}
	}
	// Remove non-desired pending (false) modules. Clean their outbox activations.
	for mod, active := range link.Modules {
		if !desired[mod] && !active {
			delete(link.Modules, mod)
			if mods := n.moduleOutbox[peer]; mods != nil {
				delete(mods, mod)
				if len(mods) == 0 { delete(n.moduleOutbox, peer) }
			}
		}
	}
	// Queue deactivation for non-desired active (true) modules.
	// They stay in Modules until response confirms.
	for mod, active := range link.Modules {
		if !desired[mod] && active {
			if n.moduleOutbox[peer] == nil { n.moduleOutbox[peer] = map[string]bool{} }
			n.moduleOutbox[peer][mod] = false
		}
	}
	n.kick(peer)
	return Enc("ok"), nil
}
