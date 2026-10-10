package core

import (
	"encoding/json"
	"strings"
	"log"
	"net/http"
)



func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	sigDomain, sig, body, err := ReadP2P(r)
	if err != nil { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	var env SyncRequest
	if json.Unmarshal(body, &env) != nil { HttpError(w, http.StatusBadRequest, "malformed"); return }
	if !IdentityInDomain(env.From, sigDomain) { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	n := s.nodeByIdentity(env.To)
	if n == nil { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	n.mu.Lock()
	if n.links[env.From] == nil { n.mu.Unlock(); HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	n.mu.Unlock()
	if err := s.verifyP2PSignature(sigDomain, r.URL.Path, sig, body); err != nil { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }

	n.mu.Lock()
	link := n.links[env.From]
	if link == nil { n.mu.Unlock(); HttpError(w, http.StatusNotFound, "no link"); return }

	var kicks []string
	collision := link.Sync.Pending != nil

	// Piggybacked response.
	if env.Resp != nil && link.Sync.Pending != nil && env.Resp.Counter == link.Sync.Counter {
		method := link.Sync.Pending.Method
		if coreSyncMethods[method] {
			n.handleCoreSyncResponse(env.From, link, method, link.Sync.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		} else if idx, ok := s.syncRoute[method]; ok {
			sent := *link.Sync.Pending; sent.Method = strings.TrimPrefix(sent.Method, s.regs[idx].name+".")
			n.modules[idx].HandleResponse(env.From, link, sent, env.Resp.OK, env.Resp.Timestamp)
		}
		link.Sync.Counter++; link.Sync.RespTimestamp = env.Resp.Timestamp
		piggyKicks := n.drainAllKicks(); piggyMsgs := n.drainDeferredMsgs(); link.Sync.Pending = nil
		if msg := n.nextTx(env.From); msg != nil { link.Sync.Pending = msg; piggyKicks = append(piggyKicks, env.From) }
		if err := n.commitLocked(); err != nil { n.mu.Unlock(); HttpError(w, http.StatusInternalServerError, "persist failed"); return }
		for _, m := range piggyMsgs { n.srv.sendMessage(n.id, m.To, m.Method, m.Payload) }
		for _, p := range piggyKicks { n.kick(p) }
		collision = false
	}

	if env.Counter < link.Sync.Counter {
		if cr := link.Sync.CachedResponse; cr != nil && cr.Counter == env.Counter { resp := *cr; n.mu.Unlock(); WriteJSON(w, http.StatusOK, resp); return }
		n.mu.Unlock(); HttpError(w, http.StatusConflict, "stale counter"); return
	}
	if env.Counter > link.Sync.Counter { n.mu.Unlock(); HttpError(w, http.StatusConflict, "counter ahead"); return }
	if collision && link.Sync.Dominant() { n.mu.Unlock(); HttpError(w, http.StatusConflict, "collision"); return }

	ts := env.Timestamp; if link.Sync.Dominant() { ts = NowSecs() }

	// Dispatch: core methods first, then module.
	var ok bool
	if coreSyncMethods[env.Method] {
		var reqErr error
		ok, reqErr = n.handleCoreSyncRequest(env.From, link, env.Method, env.Payload, ts)
		if reqErr != nil { n.mu.Unlock(); HttpError(w, http.StatusBadRequest, reqErr.Error()); return }
	} else if idx, found := s.syncRoute[env.Method]; found {
		regName := s.regs[idx].name
		if !link.ModuleEnabled(regName) {
			n.mu.Unlock(); HttpError(w, http.StatusBadRequest, "module not active on link"); return
		}
		var reqErr error
		shortMethod := strings.TrimPrefix(env.Method, regName+".")
		ok, reqErr = n.modules[idx].HandleRequest(env.From, link, shortMethod, env.Payload, ts)
		if reqErr != nil { n.mu.Unlock(); HttpError(w, http.StatusBadRequest, reqErr.Error()); return }
	} else {
		n.mu.Unlock(); HttpError(w, http.StatusBadRequest, "unknown sync method"); return
	}

	link.Sync.Counter++; link.Sync.ReqTimestamp = ts
	var deferredMsgs []Msg
	if ok { kicks = append(kicks, n.drainAllKicks()...); deferredMsgs = n.drainDeferredMsgs() }
	if collision { link.Sync.Pending = nil }

	resp := SyncResponse{Counter: env.Counter, OK: ok, Timestamp: ts}
	link.Sync.CachedResponse = &resp
	if err := n.commitLocked(); err != nil { n.mu.Unlock(); HttpError(w, http.StatusInternalServerError, "persist failed"); return }
	for _, m := range deferredMsgs { n.srv.sendMessage(n.id, m.To, m.Method, m.Payload) }
	kicks = append(kicks, env.From); n.mu.Unlock()
	WriteJSON(w, http.StatusOK, resp)
	for _, p := range kicks { n.kick(p) }
}

func (n *node) sendPending(peer string) {
	n.mu.Lock()
	link := n.links[peer]
	if link == nil || n.inFlight[peer] { n.mu.Unlock(); return }
	promoted := false
	if link.Sync.Pending == nil {
		msg := n.nextTx(peer); if msg == nil { n.mu.Unlock(); return }
		link.Sync.Pending = msg; promoted = true
	}
	n.inFlight[peer] = true
	msg := link.Sync.Pending; counter := link.Sync.Counter
	env := SyncRequest{From: n.id, To: peer, Counter: counter, Method: msg.Method, Payload: msg.Payload, Timestamp: NowSecs()}
	if cr := link.Sync.CachedResponse; cr != nil { env.Resp = cr }
	if promoted { if err := n.commitLocked(); err != nil { delete(n.inFlight, peer); n.mu.Unlock(); return } }
	n.mu.Unlock()

	status, body, err := n.srv.postSigned(PeerDomain(peer), "/p2p/tx", env)

	n.mu.Lock()
	delete(n.inFlight, peer)
	if err != nil || status != http.StatusOK {
		if status == http.StatusBadRequest { log.Printf("[creditnet] %s→%s %s: 400", n.id, peer, msg.Method) }
		link = n.links[peer]; requeue := status == http.StatusConflict && link != nil && link.Sync.Pending == nil
		n.mu.Unlock(); if requeue { n.kick(peer) }; return
	}
	var resp SyncResponse
	if json.Unmarshal(body, &resp) != nil { n.mu.Unlock(); return }
	link = n.links[peer]
	if link == nil || link.Sync.Pending != msg || link.Sync.Counter != counter || resp.Counter != counter { n.mu.Unlock(); return }

	method := msg.Method
	if coreSyncMethods[method] {
		n.handleCoreSyncResponse(peer, link, method, msg.Payload, resp.OK, resp.Timestamp)
	} else if idx, ok := n.srv.syncRoute[method]; ok {
		sent := *msg; sent.Method = strings.TrimPrefix(sent.Method, n.srv.regs[idx].name+".")
		n.modules[idx].HandleResponse(peer, link, sent, resp.OK, resp.Timestamp)
	}

	link.Sync.Counter++; link.Sync.RespTimestamp = resp.Timestamp
	kicks := n.drainAllKicks(); deferredMsgs := n.drainDeferredMsgs(); link.Sync.Pending = nil
	if next := n.nextTx(peer); next != nil { link.Sync.Pending = next; kicks = append(kicks, peer) }
	if err := n.commitLocked(); err != nil { n.mu.Unlock(); return }
	for _, m := range deferredMsgs { n.srv.sendMessage(n.id, m.To, m.Method, m.Payload) }
	n.mu.Unlock()
	for _, p := range kicks { n.kick(p) }
}
