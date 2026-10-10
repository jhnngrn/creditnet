package core

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	Domain  string
	Listen  string
	DataDir string
	Store   Store

	RetryInterval time.Duration
	SessionTTL    time.Duration
	KeyCacheTTL   time.Duration
	HTTPTimeout   time.Duration
	PeerScheme    string

	AdminUser string
	AdminPass string
	Verbose   bool
}

func (c *Config) fill() error {
	if c.Domain == "" { return errors.New("Config.Domain is required") }
	c.Domain = strings.ToLower(c.Domain)
	if c.Listen == "" {
		if _, port, err := net.SplitHostPort(c.Domain); err == nil { c.Listen = ":" + port } else { c.Listen = ":8471" }
	}
	if c.RetryInterval <= 0 { c.RetryInterval = 60 * time.Second }
	if c.SessionTTL <= 0 { c.SessionTTL = 24 * time.Hour }
	if c.KeyCacheTTL <= 0 { c.KeyCacheTTL = time.Hour }
	if c.HTTPTimeout <= 0 { c.HTTPTimeout = 10 * time.Second }
	if c.PeerScheme == "" { c.PeerScheme = "https" }
	return nil
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

// Core-owned sync methods — modules cannot register these.
var coreSyncMethods = map[string]bool{"sync_trustline": true, "payment": true, "sync_demurrage": true, "transfer": true, "sync_module": true}
var coreMsgMethods = map[string]bool{"link_request": true}

type registration struct {
	name    string
	factory ModuleFactory
	sync    map[string]bool
	msg     map[string]bool
	account map[string]bool
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

type Server struct {
	cfg    Config
	store  Store
	key    ed25519.PrivateKey
	client *http.Client
	mux    *http.ServeMux

	regs         []registration
	syncRoute    map[string]int // method → index in regs
	msgRoute     map[string]int // link-required messages
	openMsgRoute map[string]int // messages that don't require a link
	acctRoute    map[string]int

	nmu   sync.RWMutex
	nodes map[string]*node

	umu      sync.Mutex
	users    map[string]*UserRecord
	sessions map[string]*session

	kmu      sync.Mutex
	keyCache map[string]cachedKey
}

func NewServer(cfg Config) (*Server, error) {
	if err := cfg.fill(); err != nil { return nil, err }
	store := cfg.Store
	if store == nil {
		var err error
		if store, err = NewFileStore(cfg.DataDir); err != nil { return nil, err }
	}
	key, err := loadOrCreateKey(store)
	if err != nil { return nil, err }
	users, err := store.LoadUsers()
	if err != nil { return nil, err }
	return &Server{
		cfg: cfg, store: store, key: key,
		client:    &http.Client{Timeout: cfg.HTTPTimeout},
		syncRoute: map[string]int{}, msgRoute: map[string]int{}, openMsgRoute: map[string]int{}, acctRoute: map[string]int{},
		nodes: map[string]*node{}, users: users,
		sessions: map[string]*session{}, keyCache: map[string]cachedKey{},
	}, nil
}

// Register adds a module. Methods are prefixed with "name." on the wire.
// msgMethods require the sender to be a linked peer; openMsgMethods do not.
func (s *Server) Register(name string, factory ModuleFactory, syncMethods, msgMethods, openMsgMethods, accountMethods []string) error {
	idx := len(s.regs)
	for _, m := range syncMethods {
		wm := name + "." + m
		if coreSyncMethods[wm] { return fmt.Errorf("module %q: sync method %q is owned by core", name, m) }
		if _, ok := s.syncRoute[wm]; ok { return fmt.Errorf("module %q: sync method %q already registered", name, m) }
	}
	allMsg := append(msgMethods, openMsgMethods...)
	for _, m := range allMsg {
		wm := name + "." + m
		if coreMsgMethods[wm] { return fmt.Errorf("module %q: msg method %q is owned by core", name, m) }
		if _, ok := s.msgRoute[wm]; ok { return fmt.Errorf("module %q: msg method %q already registered", name, m) }
		if _, ok := s.openMsgRoute[wm]; ok { return fmt.Errorf("module %q: msg method %q already registered", name, m) }
	}
	for _, m := range accountMethods {
		wm := name + "." + m
		if _, ok := s.acctRoute[wm]; ok { return fmt.Errorf("module %q: account method %q already registered", name, m) }
	}
	r := registration{name: name, factory: factory, sync: setFrom(syncMethods), msg: setFrom(allMsg), account: setFrom(accountMethods)}
	s.regs = append(s.regs, r)
	for _, m := range syncMethods { s.syncRoute[name+"."+m] = idx }
	for _, m := range msgMethods { s.msgRoute[name+"."+m] = idx }
	for _, m := range openMsgMethods { s.openMsgRoute[name+"."+m] = idx }
	for _, m := range accountMethods { s.acctRoute[name+"."+m] = idx }
	return nil
}

// MustRegister calls Register and panics on error.
func (s *Server) MustRegister(name string, factory ModuleFactory, syncMethods, msgMethods, openMsgMethods, accountMethods []string) {
	if err := s.Register(name, factory, syncMethods, msgMethods, openMsgMethods, accountMethods); err != nil {
		panic(err)
	}
}

func setFrom(ss []string) map[string]bool {
	m := map[string]bool{}; for _, s := range ss { m[s] = true }; return m
}

func (s *Server) Init() error {
	ids, err := s.store.ListNodes()
	if err != nil { return err }
	for _, id := range ids {
		raw, err := s.store.LoadNode(id)
		if err != nil { return err }
		user, domain, err := ParseIdentity(id)
		if err != nil || domain != s.cfg.Domain { continue }
		s.nodes[user] = s.makeNode(id, raw)
	}
	// Kick peers with pending outbox entries from boot migration.
	for _, n := range s.nodes {
		for peer := range n.moduleOutbox { n.kick(peer) }
	}
	if s.cfg.AdminUser != "" && s.cfg.AdminPass != "" {
		if err := s.bootstrapAdmin(s.cfg.AdminUser, s.cfg.AdminPass); err != nil { return err }
	}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("GET /p2p/key", s.handleKey)
	s.mux.HandleFunc("POST /p2p/tx", s.handleTx)
	s.mux.HandleFunc("POST /p2p/msg", s.handleMsg)
	s.mux.HandleFunc("POST /api/auth", s.handleAuth)
	s.mux.HandleFunc("POST /api/admin", s.handleAdmin)
	s.mux.HandleFunc("POST /api/account", s.handleAccount)
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen); if err != nil { return err }
	return s.Serve(ctx, ln)
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	hs := &http.Server{Handler: s.mux}
	tctx, cancel := context.WithCancel(ctx); defer cancel()
	go s.retryLoop(tctx)
	done := make(chan struct{})
	go func() { defer close(done); <-tctx.Done(); sc, c := context.WithTimeout(context.Background(), 3*time.Second); defer c(); hs.Shutdown(sc) }()
	s.logf("listening on %s as domain %s", ln.Addr(), s.cfg.Domain)
	err := hs.Serve(ln); cancel(); <-done
	if errors.Is(err, http.ErrServerClosed) { return nil }
	return err
}

func (s *Server) logf(format string, args ...any) {
	if s.cfg.Verbose { log.Printf("[creditnet %s] "+format, append([]any{s.cfg.Domain}, args...)...) }
}

func (s *Server) getNode(user string) *node { s.nmu.RLock(); defer s.nmu.RUnlock(); return s.nodes[user] }

func (s *Server) nodeByIdentity(id string) *node {
	user, domain, err := ParseIdentity(id)
	if err != nil || domain != s.cfg.Domain { return nil }
	return s.getNode(user)
}

func (s *Server) retryLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.RetryInterval); defer t.Stop()
	for { select { case <-ctx.Done(): return; case <-t.C: s.retryPending() } }
}

func (s *Server) retryPending() {
	s.nmu.RLock()
	nodes := make([]*node, 0, len(s.nodes)); for _, n := range s.nodes { nodes = append(nodes, n) }
	s.nmu.RUnlock()
	for _, n := range nodes {
		n.mu.Lock()
		var kicks []string
		for peer, l := range n.links { if l.Sync.Pending != nil && !n.inFlight[peer] { kicks = append(kicks, peer) } }
		n.mu.Unlock()
		for _, p := range kicks { n.kick(p) }
	}
}

// ---------------------------------------------------------------------------
// node — owns Links, limitOutbox, linkRequests; modules plug in
// ---------------------------------------------------------------------------

type node struct {
	srv      *Server
	mu       sync.Mutex
	id       string
	links    map[string]*Link
	modules  []Module
	inFlight map[string]bool

	// Core-owned state (shared concerns, not duplicated in modules).
	limitOutbox     map[string]uint64
	directOutbox    map[string]uint64
	demurrageOutbox map[string]uint64
	transferOutbox  map[string]uint64
	moduleOutbox    map[string]map[string]bool // peer → module → activate(true)/deactivate(false)
		linkRequests  map[string]int64
	receipts      []Receipt
	deferredMsgs  []Msg
}

type nodeState struct {
	ID       string                     `json:"id"`
	Links    map[string]*Link           `json:"links"`
	Receipts []Receipt                  `json:"receipts,omitempty"`
	Mods     map[string]json.RawMessage `json:"mods,omitempty"`
}

func (s *Server) makeNode(id string, raw json.RawMessage) *node {
	var st nodeState
	if raw != nil { json.Unmarshal(raw, &st) }
	if st.Links == nil { st.Links = map[string]*Link{} }
	if st.Mods == nil { st.Mods = map[string]json.RawMessage{} }
	n := &node{
		srv: s, id: id, links: st.Links, receipts: st.Receipts,
		modules: make([]Module, len(s.regs)), inFlight: map[string]bool{},
		limitOutbox: map[string]uint64{}, directOutbox: map[string]uint64{}, demurrageOutbox: map[string]uint64{}, transferOutbox: map[string]uint64{}, moduleOutbox: map[string]map[string]bool{}, linkRequests: map[string]int64{},
	}
	// Migrate links with nil Modules (pre-bilateral-activation data).
	for _, link := range n.links {
		if link.Modules == nil {
			link.Modules = map[string]bool{}
			for _, r := range s.regs { link.Modules[r.name] = false }
		}
	}
	// Populate activation outbox from pending (false) modules — retry on boot.
	for peer, link := range n.links {
		for mod, active := range link.Modules {
			if !active {
				if n.moduleOutbox[peer] == nil { n.moduleOutbox[peer] = map[string]bool{} }
				n.moduleOutbox[peer][mod] = true
			}
		}
	}
	for i, r := range s.regs {
		mod := r.factory(id, n.links, st.Mods[r.name])
		mod.SetCallbacks(r.name, n.moduleSendFunc(r.name), n.deferSendFunc(), n.scheduleFunc(), n.bwFunc(), n.pbFunc(), n.receiptFunc(), n.transferFunc())
		mod.ScheduleTimers()
		n.modules[i] = mod
	}
	return n
}

func (n *node) commitLocked() error {
	st := nodeState{ID: n.id, Links: n.links, Receipts: n.receipts, Mods: map[string]json.RawMessage{}}
	for i, r := range n.srv.regs { data, _ := json.Marshal(n.modules[i]); st.Mods[r.name] = data }
	if err := n.srv.store.SaveNode(n.id, st); err != nil {
		if raw, lerr := n.srv.store.LoadNode(n.id); lerr == nil {
			reloaded := n.srv.makeNode(n.id, raw); n.links = reloaded.links; n.modules = reloaded.modules
			n.limitOutbox = reloaded.limitOutbox; n.directOutbox = reloaded.directOutbox; n.demurrageOutbox = reloaded.demurrageOutbox; n.transferOutbox = reloaded.transferOutbox; n.moduleOutbox = reloaded.moduleOutbox; n.linkRequests = reloaded.linkRequests; n.receipts = reloaded.receipts
		}
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Bandwidth — core computes, modules report Reserved
// ---------------------------------------------------------------------------

func (n *node) kick(peer string) { go n.sendPending(peer) }

func (n *node) scheduleFunc() ScheduleFunc {
	return func(after time.Duration, fn func()) *time.Timer {
		return time.AfterFunc(after, func() {
			n.mu.Lock(); fn(); kicks := n.drainAllKicks()
			if err := n.commitLocked(); err != nil { log.Printf("[creditnet] %s: schedule persist: %v", n.id, err) }
			n.mu.Unlock()
			for _, p := range kicks { n.kick(p) }
		})
	}
}

func (n *node) sendFunc() SendFunc {
	return func(peer, method string, payload json.RawMessage) { n.srv.sendMessage(n.id, peer, method, payload) }
}

func (n *node) moduleSendFunc(modName string) SendFunc {
	return func(peer, method string, payload json.RawMessage) { n.srv.sendMessage(n.id, peer, modName+"."+method, payload) }
}

func (n *node) accountOps() AccountOps {
	return AccountOps{
		SendMessage: n.sendFunc(),
		AddLink: func(peer string) error {
			if _, _, err := ParseIdentity(peer); err != nil { return err }
			peer = strings.ToLower(peer)
			if peer == n.id { return fmt.Errorf("cannot link to self") }
			if n.links[peer] != nil { return fmt.Errorf("link to %s already exists", peer) }
			if len(n.links) >= MaxLinks { return fmt.Errorf("max links reached") }
			n.links[peer] = NewLink(n.id, peer)
			return nil
		},
		RemoveLink: func(peer string) error {
			peer = strings.ToLower(peer)
			if n.links[peer] == nil { return fmt.Errorf("no link to %s", peer) }
			for _, mod := range n.modules { mod.OnRemoveLink(peer) }
			delete(n.links, peer); delete(n.limitOutbox, peer); delete(n.directOutbox, peer); delete(n.demurrageOutbox, peer); delete(n.transferOutbox, peer); delete(n.moduleOutbox, peer)
			return nil
		},
	}
}
