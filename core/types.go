package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

func ParseIdentity(id string) (user, domain string, err error) {
	id = strings.ToLower(id)
	i := strings.Index(id, "@")
	if i <= 0 || i == len(id)-1 {
		return "", "", fmt.Errorf("invalid identity %q (want user@domain)", id)
	}
	user, domain = id[:i], id[i+1:]
	if !ValidUsername(user) {
		return "", "", fmt.Errorf("invalid username %q", user)
	}
	return user, domain, nil
}

func ValidUsername(u string) bool {
	if len(u) == 0 || len(u) > 64 { return false }
	for _, c := range u {
		if !(c == '_' || c == '-' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) { return false }
	}
	return true
}

func PeerDomain(peer string) string {
	_, d, err := ParseIdentity(peer)
	if err != nil { return "" }
	return d
}

func IdentityInDomain(identity, domain string) bool {
	_, d, err := ParseIdentity(identity)
	return err == nil && d == domain
}

// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

type SyncRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *SyncResponse   `json:"resp,omitempty"`
}

type SyncResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

type MsgEnvelope struct {
	From    string          `json:"from"`
	To      string          `json:"to"`
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// ---------------------------------------------------------------------------
// Sync state + Link
// ---------------------------------------------------------------------------

type QueuedMsg struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Msg is a fire-and-forget message with a destination.
// Used for deferred messages that modules accumulate during sync handlers
// and core sends after persist.
type Msg struct {
	To      string          `json:"to"`
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type SyncState struct {
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Pending        *QueuedMsg    `json:"pending,omitempty"`
	CachedResponse *SyncResponse `json:"cached_response,omitempty"`
	ReqTimestamp   int64         `json:"req_ts"`
	RespTimestamp  int64         `json:"resp_ts"`
}

func (s *SyncState) Dominant() bool { return s.Counter%2 == s.TurnBit }

type Link struct {
	Peer string    `json:"peer"`
	Sync SyncState `json:"sync"`

	Balance       int64  `json:"balance"`
	CreditLimit   uint64 `json:"credit_limit"`
	CreditLimitIn uint64 `json:"credit_limit_in"`

	// Demurrage: rate in promille per year (10 = 1%/year). 0 = off.
	// Demurrage applies to peer's debt to me; DemurrageIn applies to my debt to peer.
	Demurrage     uint64 `json:"demurrage"`
	DemurrageIn   uint64 `json:"demurrage_in"`
	DemurrageTime int64  `json:"demurrage_time"` // last materialized (seconds)

	Modules       map[string]bool `json:"modules"` // module → active(true)/pending(false)
}

// ModuleEnabled checks if a module is bilaterally active for this link.
func (l *Link) ModuleEnabled(mod string) bool {
	return l.Modules[mod]
}

func NewLink(self, peer string) *Link {
	var bit uint64
	if self > peer { bit = 1 }
	return &Link{Peer: peer, Sync: SyncState{TurnBit: bit}}
}

// ---------------------------------------------------------------------------
// Side constants — used by core bandwidth and Module.Reserved
// ---------------------------------------------------------------------------

const (
	IN  = 0
	OUT = 1
)

// ---------------------------------------------------------------------------
// Callbacks
// ---------------------------------------------------------------------------

type SendFunc func(peer, method string, payload json.RawMessage)
type DeferSendFunc func(to, method string, payload json.RawMessage)
type ScheduleFunc func(after time.Duration, fn func()) *time.Timer

// BandwidthFunc returns available bandwidth on a link (credit_limit + pending_balance).
type BandwidthFunc func(peer string, side int) uint64

// PendingBalanceFunc returns the effective debt on a link after all modules'
// reservations are subtracted. Triad uses this: it asks "how much debt
// can I clear?" which is pending_balance, not bandwidth.
type PendingBalanceFunc func(peer string, side int) int64


// ReceiptFunc logs a completed operation. Core owns the receipt buffer.
type ReceiptFunc func(id, peer, module string, amount int64, ok bool)

// TransferFunc buffers a non-receipted balance transfer to a peer. Core accumulates and syncs via transfer.
type TransferFunc func(peer string, amount uint64)

type Receipt struct {
	ID        string `json:"id"`
	Peer      string `json:"peer"`
	Amount    int64  `json:"amount"`
	Status    bool   `json:"status"`
	Timestamp int64  `json:"timestamp"`
	Module    string `json:"module,omitempty"`
}

const MaxReceipts = 64
const TransferThreshold = 256 // don't sync transfer below this

type AccountOps struct {
	SendMessage SendFunc
	AddLink     func(peer string) error
	RemoveLink  func(peer string) error
}

// ---------------------------------------------------------------------------
// Queue helpers — used by settlement, loops, triad
// ---------------------------------------------------------------------------

func Enqueue(q *[]string, method string) {
	*q = append(*q, method)
}

func Dequeue(q *[]string) {
	if len(*q) > 0 { *q = (*q)[1:] }
}

func QueueHead(q []string) string {
	if len(q) > 0 { return q[0] }
	return ""
}

func QueueHas(q []string, method string) bool {
	for _, m := range q { if m == method { return true } }
	return false
}

// ---------------------------------------------------------------------------
// Module interface
// ---------------------------------------------------------------------------

type Module interface {
	HandleRequest(peer string, ls *Link, method string, payload json.RawMessage, ts int64) (ok bool, err error)
	HandleResponse(peer string, ls *Link, sent QueuedMsg, ok bool, ts int64)
	HandleMessage(sender, method string, payload json.RawMessage) bool
	HandleAccountRequest(method string, payload json.RawMessage, ops AccountOps) (json.RawMessage, error)

	// NextTx returns the next message to load into the sync slot for this peer.
	NextTx(peer string) *QueuedMsg
	DrainKicks() []string

	// Reserved reports how much capacity this module has locked on a link.
	// Core sums this across all modules to compute available bandwidth.
	Reserved(peer string, side int) uint64

	// Status returns module-specific status (merged into the node status response).
	Status() json.RawMessage

	SetCallbacks(module string, send SendFunc, deferSend DeferSendFunc, schedule ScheduleFunc, bw BandwidthFunc, pb PendingBalanceFunc, receipt ReceiptFunc, transfer TransferFunc)
	ScheduleTimers()
	OnRemoveLink(peer string)
}

type ModuleFactory func(nodeID string, links map[string]*Link, state json.RawMessage) Module

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func Enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }
func NowSecs() int64 { return time.Now().Unix() }

const (
	MaxUsers    = 1000
	MaxLinks    = 64
	MaxKeyCache = 256
)
