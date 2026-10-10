package core

import (
	"encoding/json"
	"strings"
	"net/http"
)

func (s *Server) handleMsg(w http.ResponseWriter, r *http.Request) {
	sigDomain, sig, body, err := ReadP2P(r)
	if err != nil { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	var env MsgEnvelope
	if json.Unmarshal(body, &env) != nil { HttpError(w, http.StatusBadRequest, "malformed"); return }
	if !IdentityInDomain(env.From, sigDomain) { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	if err := s.verifyP2PSignature(sigDomain, r.URL.Path, sig, body); err != nil { HttpError(w, http.StatusUnauthorized, "unauthorized"); return }
	n := s.nodeByIdentity(env.To)
	if n == nil { w.WriteHeader(http.StatusOK); return }
	n.mu.Lock()
	persist := false
	// Core-owned messages first.
	if coreMsgMethods[env.Method] {
		n.handleCoreMsgRequest(env.From, env.Method)
	} else if idx, ok := s.msgRoute[env.Method]; ok {
		link := n.links[env.From]
		if link != nil && link.ModuleEnabled(s.regs[idx].name) {
			persist = n.modules[idx].HandleMessage(env.From, strings.TrimPrefix(env.Method, s.regs[idx].name+"."), env.Payload)
		}
	} else if idx, ok := s.openMsgRoute[env.Method]; ok {
		link := n.links[env.From]
		if link == nil || link.ModuleEnabled(s.regs[idx].name) {
			persist = n.modules[idx].HandleMessage(env.From, strings.TrimPrefix(env.Method, s.regs[idx].name+"."), env.Payload)
		}
	}
	kicks := n.drainAllKicks()
	if persist { n.commitLocked() }
	n.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	for _, p := range kicks { n.kick(p) }
}

func (s *Server) sendMessage(from, to, method string, payload json.RawMessage) {
	env := MsgEnvelope{From: from, To: to, Method: method, Payload: payload}
	go s.postSigned(PeerDomain(to), "/p2p/msg", env)
}
