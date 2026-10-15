// Package link defines the bilateral sync primitive — the data structures,
// protocol types and sync loop for bilateral links between parties.
package link

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Errors — protocol-level errors
// ---------------------------------------------------------------------------

var (
	ErrStaleCounter    = errors.New("stale counter")
	ErrCounterAhead    = errors.New("counter ahead")
	ErrCollision       = errors.New("collision")
	ErrRefsHeld        = errors.New("close with refs > 0")
	ErrNoLink          = errors.New("no link")
	ErrMaxLinks        = errors.New("max links")
	ErrInvitesDisabled = errors.New("invites disabled")
)

// ---------------------------------------------------------------------------
// Link — bilateral sync state
// ---------------------------------------------------------------------------

// LinkStatus — what the link IS.
type ConnectionStatus string

const (
	Waiting   ConnectionStatus = "waiting"   // proposed, awaiting peer decision
	Incoming  ConnectionStatus = "incoming"  // peer proposed, awaiting local decision
	Connected ConnectionStatus = "connected" // bilaterally established
)

// ConnectionState is the bilateral connection state: what IS (Status),
// what to DO next (IntentConnect/IntentClose), and who holds it open (Refs).
// Close is deferred while Refs > 0.
type ConnectionState struct {
	Status        ConnectionStatus `json:"status,omitempty"`
	IntentConnect bool             `json:"intent_connect,omitempty"`
	IntentClose   bool             `json:"intent_close,omitempty"`
	Refs          uint             `json:"refs,omitempty"`
}

// Ref increments the reference count.
func (c *ConnectionState) Ref()   { c.Refs++ }

// Unref decrements the reference count.
func (c *ConnectionState) Unref() { if c.Refs > 0 { c.Refs-- } }

// UnrefClose decrements refs and sets IntentClose when refs reach 0.
func (c *ConnectionState) UnrefClose() {
	if c.Refs > 0 {
		c.Refs--
	}
	if c.Refs == 0 {
		c.IntentClose = true
	}
}

// LinkState is a ConnectionState (for the link itself) plus per-module connections.
type LinkState struct {
	ConnectionState
	Modules map[string]*ConnectionState `json:"modules,omitempty"`
}

// Module processes sync methods and account API calls for a namespace.
// HandleRequest returns (ok, kicks, err) where kicks is a list of peers
// that need kicking on other links (cross-link side effects).
// HandleResponse returns kicks for the same purpose.
// HandleAccount handles user-facing API calls.
// NextTx returns the next queued message to send on a peer, or nil.
type Module interface {
	Name() string
	HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)
	HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string
	HandleAccount(method string, payload json.RawMessage) (json.RawMessage, error)
	NextTx(peer string) *QueuedMsg
}

// QueuedMsg is a message queued for sending on a link.
// QueuedAt is a unix-second timestamp of when the message was queued.
// When multiple modules return a candidate for the same peer, the
// oldest QueuedAt wins. Zero means unset — unset always loses to any
// set value; when both are unset (or equal), registration order wins.
type QueuedMsg struct {
	Method   string          `json:"method"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	QueuedAt int64           `json:"queued_at,omitempty"`
}

// PeerSend tracks the outgoing send lifecycle for a link at a given counter.
// InFlight means a goroutine is blocked on Transport.Send.
// After the send completes, a retry timer is set: Refractory after error
// (blocks kicks), non-refractory after success (fallback, kicks bypass).
// Both are scoped to Counter — if counter advances via HandleSync,
// the entry is stale and ignored.
type PeerSend struct {
	Counter    uint64
	InFlight   bool
	Timer      *time.Timer
	Refractory bool
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState     `json:"state"`
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Pending        *QueuedMsg    `json:"pending,omitempty"`
	CachedResponse *SyncResponse `json:"cached_response,omitempty"`
	ReqTimestamp   int64         `json:"req_ts"`
	RespTimestamp  int64         `json:"resp_ts"`
	Send           *PeerSend     `json:"-"`
}

// ModuleEnabled returns true if the named module is bilaterally active on this link.
func (l *Link) ModuleEnabled(name string) bool {
	c := l.State.Modules[name]
	return c != nil && c.Status == Connected
}

// Dominant returns true when this side owns the current counter slot.
func (l *Link) Dominant() bool { return l.Counter%2 == l.TurnBit }

// NewLink creates a Link with the turn bit derived from identity ordering.
func NewLink(self, peer string) *Link {
	var bit uint64
	if self > peer {
		bit = 1
	}
	return &Link{TurnBit: bit}
}

// ---------------------------------------------------------------------------
// Links — collection of links for a node, including the sync loop
// ---------------------------------------------------------------------------

// EngineConfig provides the callbacks Links needs to run the sync loop.
type EngineConfig struct {
	// Send delivers a sync message to a peer and returns the response body.
	Send func(peer string, msg SyncRequest) ([]byte, error)
	// Commit persists the current state. Called under the mutex.
	Commit func()
	// Now returns the current time as unix seconds.
	Now func() int64
	// RetryDelay is the delay between retry attempts.
	RetryDelay time.Duration
}

// Links manages the set of bilateral links for a node.
type Links struct {
	mu         sync.Mutex
	m          map[string]*Link
	self       string
	InviteMode InviteMode
	Supported  map[string]struct{}
	moduleMap  map[string]Module
	engine     EngineConfig
}

// NewLinks creates a Links collection for the given node identity.
func NewLinks(self string, m map[string]*Link, mode InviteMode, supported map[string]struct{}, engine EngineConfig) *Links {
	if m == nil {
		m = map[string]*Link{}
	}
	if mode == "" {
		mode = InviteModeInvite
	}
	if supported == nil {
		supported = map[string]struct{}{}
	}
	if engine.RetryDelay == 0 {
		engine.RetryDelay = 60 * time.Second
	}
	return &Links{m: m, self: self, InviteMode: mode, Supported: supported, moduleMap: map[string]Module{}, engine: engine}
}

// Lock/Unlock expose the mutex for callers that need to hold it
// across multiple operations (e.g. HandleAccount + Commit).
func (ls *Links) Lock()   { ls.mu.Lock() }
func (ls *Links) Unlock() { ls.mu.Unlock() }

// Commit persists the current state via the engine callback. Caller must hold Lock.
func (ls *Links) Commit() { ls.engine.Commit() }

// Self returns the node identity.
func (ls *Links) Self() string { return ls.self }

// RegisterModule adds a module to the link collection.
// Its name is added to Supported so module negotiation includes it.
func (ls *Links) RegisterModule(m Module) {
	name := m.Name()
	ls.moduleMap[name] = m
	ls.Supported[name] = struct{}{}
}

// ModuleMap returns the registered modules for serialization.
func (ls *Links) ModuleMap() map[string]Module { return ls.moduleMap }

func (ls *Links) Get(peer string) *Link { return ls.m[peer] }
func (ls *Links) Len() int              { return len(ls.m) }
func (ls *Links) Full() bool            { return len(ls.m) >= MaxLinks }
func (ls *Links) Delete(peer string)    { delete(ls.m, peer) }
func (ls *Links) Map() map[string]*Link { return ls.m }

// Create adds a new link to peer and returns it.
func (ls *Links) Create(peer string) *Link {
	lk := NewLink(ls.self, peer)
	ls.m[peer] = lk
	return lk
}

// ---------------------------------------------------------------------------
// Sync lifecycle — request/response validation
// ---------------------------------------------------------------------------

// ValidateRequest checks whether an incoming request at the given counter
// is valid for this link. Returns:
//   - (*SyncResponse, nil) — stale counter but cached response exists
//   - (nil, error) — invalid (stale without cache, ahead, or collision)
//   - (nil, nil) — valid, proceed with dispatch
func (l *Link) ValidateRequest(counter uint64) (*SyncResponse, error) {
	if counter < l.Counter {
		if l.CachedResponse != nil && l.CachedResponse.Counter == counter {
			resp := *l.CachedResponse
			return &resp, nil
		}
		return nil, ErrStaleCounter
	}
	if counter > l.Counter {
		return nil, ErrCounterAhead
	}
	if l.Pending != nil && l.Dominant() {
		return nil, ErrCollision
	}
	return nil, nil
}

// AcceptRequest finalizes an incoming request. Call after the handler
// has processed the request. Returns the response to send back.
func (l *Link) AcceptRequest(reqCounter uint64, ok bool, ts int64) *SyncResponse {
	resp := SyncResponse{Counter: reqCounter, OK: ok, Timestamp: ts}
	l.CachedResponse = &resp
	l.Counter++
	l.ReqTimestamp = ts
	l.Pending = nil
	return &resp
}

// AcceptResponse finalizes a received response to our outgoing message.
// Call after the response handler has processed the response.
func (l *Link) AcceptResponse(ts int64) {
	l.Counter++
	l.RespTimestamp = ts
	l.Pending = nil
}


// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

// SyncRequest is the wire envelope for an outgoing sync message.
type SyncRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *SyncResponse   `json:"resp,omitempty"`
}

// SyncResponse is the wire envelope for a sync response.
type SyncResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// InviteMode — node-level policy for incoming connections/modules
// ---------------------------------------------------------------------------

type InviteMode string

const (
	InviteModeClosed InviteMode = "closed" // reject all incoming
	InviteModeInvite InviteMode = "invite" // incoming, manual accept
	InviteModeOpen   InviteMode = "open"   // auto-accept all
)

const MaxLinks = 64

func enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }

func splitMethod(method string) (ns, name string) {
	if i := indexOf(method, '.'); i >= 0 {
		return method[:i], method[i+1:]
	}
	return "", method
}

func indexOf(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// OlderQueuedMsg reports whether a should win over b.
// Zero QueuedAt means unset — unset loses to any set value.
// Equal values (including both zero) return false, preserving
// the earlier candidate.
func OlderQueuedMsg(a, b *QueuedMsg) bool {
	if b == nil {
		return true
	}
	if a.QueuedAt == 0 && b.QueuedAt == 0 {
		return false
	}
	if a.QueuedAt == 0 {
		return false
	}
	if b.QueuedAt == 0 {
		return true
	}
	return a.QueuedAt < b.QueuedAt
}

// ---------------------------------------------------------------------------
// Link-level request/response handling
// ---------------------------------------------------------------------------

// HandleRequest dispatches an incoming sync request by namespace.
// Link-level methods are handled internally; registered modules are
// dispatched by name. Returns (ok, kicks, error).
func (ls *Links) HandleRequest(peer, fullMethod string, payload json.RawMessage, ts int64) (bool, []string, error) {
	ns, method := splitMethod(fullMethod)
	if ns == "link" {
		ok, err := ls.handleLinkRequest(peer, method, payload)
		return ok, nil, err
	}
	lk := ls.Get(peer)
	if lk == nil {
		return false, nil, fmt.Errorf("no link to %s", peer)
	}
	mod := ls.moduleMap[ns]
	if mod == nil || !lk.ModuleEnabled(ns) {
		return false, nil, fmt.Errorf("unknown method %q", fullMethod)
	}
	return mod.HandleRequest(peer, method, payload, ts)
}

// handleLinkRequest processes an incoming link-namespace request.
func (ls *Links) handleLinkRequest(peer, method string, payload json.RawMessage) (bool, error) {
	l := ls.Get(peer)
	if l == nil {
		return false, fmt.Errorf("no link to %s", peer)
	}
	switch method {
	case "connect":
		if l.State.IntentConnect || l.State.Status == Waiting {
			l.State.Status = Connected
			l.State.IntentConnect = false
			return true, nil
		}
		if ls.InviteMode == InviteModeOpen {
			l.State.Status = Connected
			return true, nil
		}
		return false, nil

	case "close":
		if l.State.Refs > 0 {
			return false, ErrRefsHeld
		}
		ls.Delete(peer)
		return true, nil

	case "module":
		ok, err := l.reqModule(payload, ls.Supported, ls.InviteMode)
		return ok, err
	}
	return false, fmt.Errorf("unknown link method %q", method)
}

// HandleResponse dispatches a sync response by namespace.
func (ls *Links) HandleResponse(peer, fullMethod string, payload json.RawMessage, ok bool, ts int64) []string {
	ns, method := splitMethod(fullMethod)
	if ns == "link" {
		ls.handleLinkResponse(peer, method, payload, ok)
		return nil
	}
	mod := ls.moduleMap[ns]
	if mod == nil {
		return nil
	}
	return mod.HandleResponse(peer, method, payload, ok, ts)
}

// handleLinkResponse processes a link-namespace response.
func (ls *Links) handleLinkResponse(peer, method string, payload json.RawMessage, ok bool) {
	l := ls.Get(peer)
	if l == nil {
		return
	}
	switch method {
	case "connect":
		if ok {
			l.State.Status = Connected
		} else {
			l.State.Status = Waiting
		}
		l.State.IntentConnect = false

	case "close":
		if ok {
			ls.Delete(peer)
		} else {
			l.State.IntentClose = false
		}

	case "module":
		l.respModule(payload, ok)
	}
}

// NextTx returns the next message to send on a peer link.
// Link-level messages (connect/close/module) have absolute priority.
// Among registered modules, the oldest QueuedAt wins.
func (ls *Links) NextTx(peer string) *QueuedMsg {
	// Link-level messages have absolute priority.
	if msg := ls.linkNextTx(peer); msg != nil {
		msg.Method = "link." + msg.Method
		return msg
	}
	lk := ls.Get(peer)
	if lk == nil || lk.State.Status != Connected {
		return nil
	}
	var best *QueuedMsg
	for name, mod := range ls.moduleMap {
		if !lk.ModuleEnabled(name) {
			continue
		}
		if msg := mod.NextTx(peer); msg != nil {
			msg.Method = name + "." + msg.Method
			if OlderQueuedMsg(msg, best) {
				best = msg
			}
		}
	}
	return best
}

// linkNextTx returns the next link-level message to send, or nil.
func (ls *Links) linkNextTx(peer string) *QueuedMsg {
	l := ls.Get(peer)
	if l == nil {
		return nil
	}
	if l.State.IntentClose && l.State.Refs == 0 {
		return &QueuedMsg{Method: "close"}
	}
	if l.State.IntentConnect {
		return &QueuedMsg{Method: "connect"}
	}
	if l.State.Status == Connected {
		for name, mod := range l.State.Modules {
			if mod.IntentConnect {
				return &QueuedMsg{
					Method:  "module",
					Payload: enc(map[string]string{"name": name}),
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Module negotiation (internal)
// ---------------------------------------------------------------------------

func (l *Link) reqModule(payload json.RawMessage, supported map[string]struct{}, inviteMode InviteMode) (bool, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, fmt.Errorf("bad payload")
	}
	if _, ok := supported[p.Name]; !ok {
		return false, nil // we don't speak this module
	}
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	mod := l.State.Modules[p.Name]
	// Mutual — we also want this module.
	if mod != nil && (mod.IntentConnect || mod.Status == Waiting) {
		mod.Status = Connected
		mod.IntentConnect = false
		return true, nil
	}
	// Open mode — auto-accept supported modules.
	if inviteMode == InviteModeOpen {
		if mod == nil {
			mod = &ConnectionState{}
			l.State.Modules[p.Name] = mod
		}
		mod.Status = Connected
		return true, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	if mod == nil {
		mod = &ConnectionState{}
		l.State.Modules[p.Name] = mod
	}
	mod.Status = Incoming
	return false, nil // NACK — not accepted yet
}

func (l *Link) respModule(payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return
	}
	if ok {
		mod.Status = Connected
	} else {
		mod.Status = Waiting
	}
	mod.IntentConnect = false
}

// ---------------------------------------------------------------------------
// Account operations
// ---------------------------------------------------------------------------

// HandleAccount dispatches an account API call by namespace.
// Caller must hold Lock. Returns (result, error).
// Kicks are handled internally.
func (ls *Links) HandleAccount(fullMethod string, payload json.RawMessage) (json.RawMessage, error) {
	ns, method := splitMethod(fullMethod)
	if ns == "link" {
		result, kicks, err := ls.handleLinkAccount(method, payload)
		for _, k := range kicks {
			ls.Kick(k)
		}
		return result, err
	}
	mod := ls.moduleMap[ns]
	if mod == nil {
		return nil, fmt.Errorf("unknown method %q", fullMethod)
	}
	result, err := mod.HandleAccount(method, payload)
	return result, err
}

// handleLinkAccount processes a link-namespace account operation.
func (ls *Links) handleLinkAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
	switch method {
	case "set_invite_mode":
		var p struct {
			Mode InviteMode `json:"mode"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, nil, fmt.Errorf("bad request")
		}
		switch p.Mode {
		case InviteModeClosed, InviteModeInvite, InviteModeOpen:
		default:
			return nil, nil, fmt.Errorf("invalid mode %q", p.Mode)
		}
		ls.InviteMode = p.Mode
		return enc("ok"), nil, nil

	case "set_modules":
		var p struct {
			Peer    string   `json:"peer"`
			Modules []string `json:"modules"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, nil, fmt.Errorf("bad request")
		}
		lk := ls.Get(p.Peer)
		if lk == nil {
			return nil, nil, fmt.Errorf("no link to %s", p.Peer)
		}
		lk.SetModules(p.Modules, ls.Supported)
		return enc("ok"), []string{p.Peer}, nil

	case "accept_module":
		var p struct {
			Peer   string `json:"peer"`
			Module string `json:"module"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, nil, fmt.Errorf("bad request")
		}
		lk := ls.Get(p.Peer)
		if lk == nil {
			return nil, nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err := lk.AcceptModule(p.Module); err != nil {
			return nil, nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		return enc("ok"), []string{p.Peer}, nil

	case "reject_module":
		var p struct {
			Peer   string `json:"peer"`
			Module string `json:"module"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, nil, fmt.Errorf("bad request")
		}
		lk := ls.Get(p.Peer)
		if lk == nil {
			return nil, nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err := lk.RejectModule(p.Module); err != nil {
			return nil, nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		return enc("ok"), nil, nil

	case "status":
		return ls.Status(), nil, nil
	}
	return nil, nil, fmt.Errorf("unknown link method %q", method)
}

// Status returns the link collection status as JSON.
func (ls *Links) Status() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range ls.m {
		links[peer] = map[string]any{
			"state": l.State,
		}
	}
	return enc(map[string]any{
		"links":       links,
		"invite_mode": ls.InviteMode,
	})
}

// SetModules proposes the given modules on this link, filtering by supported.
// Modules not in the desired set and not yet connected are removed.
func (l *Link) SetModules(modules []string, supported map[string]struct{}) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	desired := map[string]bool{}
	for _, name := range modules {
		if _, ok := supported[name]; ok {
			desired[name] = true
		}
	}
	for name := range desired {
		mod := l.State.Modules[name]
		if mod == nil {
			l.State.Modules[name] = &ConnectionState{IntentConnect: true}
		} else if mod.Status != Connected {
			mod.IntentConnect = true
		}
	}
	for name, mod := range l.State.Modules {
		if name == "trustline" {
			continue
		}
		if !desired[name] && mod.Status != Connected {
			delete(l.State.Modules, name)
		}
	}
}

// AcceptModule accepts an incoming module proposal.
func (l *Link) AcceptModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	mod.IntentConnect = true
	return nil
}

// RejectModule rejects an incoming module proposal.
func (l *Link) RejectModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	delete(l.State.Modules, name)
	return nil
}

// ---------------------------------------------------------------------------
// Sync loop — incoming
// ---------------------------------------------------------------------------

// QuickReject returns true if this request can be rejected before
// expensive signature verification (no existing link and not a connect/close).
func (ls *Links) QuickReject(from, method string) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.Get(from) == nil && method != "link.connect" && method != "link.close"
}

// HandleSync processes an incoming sync request. The caller is responsible
// for authentication (signature verification).
func (ls *Links) HandleSync(env SyncRequest) (*SyncResponse, error) {
	ls.mu.Lock()

	lk := ls.Get(env.From)
	if lk == nil {
		if env.Method == "link.close" {
			ls.mu.Unlock()
			return &SyncResponse{Counter: env.Counter, OK: true, Timestamp: ls.engine.Now()}, nil
		}
		if env.Method != "link.connect" {
			ls.mu.Unlock()
			return nil, ErrNoLink
		}
		if ls.InviteMode == InviteModeClosed {
			ls.mu.Unlock()
			return nil, ErrInvitesDisabled
		}
		if ls.Full() {
			ls.mu.Unlock()
			return nil, ErrMaxLinks
		}
		lk = ls.Create(env.From)
		lk.State.Status = Incoming
	}

	// Piggybacked response.
	if env.Resp != nil && lk.Pending != nil && env.Resp.Counter == lk.Counter {
		pbKicks := ls.HandleResponse(env.From, lk.Pending.Method, lk.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		lk.AcceptResponse(env.Resp.Timestamp)
		ls.engine.Commit()
		for _, k := range pbKicks {
			ls.Kick(k)
		}
	}

	// Validate counter and collision.
	if cached, err := lk.ValidateRequest(env.Counter); err != nil {
		ls.mu.Unlock()
		return nil, err
	} else if cached != nil {
		ls.mu.Unlock()
		return cached, nil
	}

	ts := env.Timestamp
	if lk.Dominant() {
		ts = ls.engine.Now()
	}

	// Dispatch to handler.
	ok, kicks, reqErr := ls.HandleRequest(env.From, env.Method, env.Payload, ts)
	if reqErr != nil {
		ls.mu.Unlock()
		return nil, reqErr
	}

	resp := lk.AcceptRequest(env.Counter, ok, ts)
	ls.engine.Commit()
	ls.mu.Unlock()
	ls.Kick(env.From)
	for _, k := range kicks {
		ls.Kick(k)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Sync loop — outgoing
// ---------------------------------------------------------------------------

// Kick triggers a sendPending for the given peer.
func (ls *Links) Kick(peer string) { go ls.sendPending(peer) }

// KickAll kicks all peers. Used at boot to resume pending work.
func (ls *Links) KickAll() {
	ls.mu.Lock()
	peers := make([]string, 0, ls.Len())
	for peer := range ls.m {
		peers = append(peers, peer)
	}
	ls.mu.Unlock()
	for _, p := range peers {
		ls.Kick(p)
	}
}

// Schedule runs fn after delay under the Links mutex.
// fn returns a list of peers to kick. Links commits and kicks.
func (ls *Links) Schedule(delay time.Duration, fn func() []string) {
	time.AfterFunc(delay, func() {
		ls.mu.Lock()
		kicks := fn()
		ls.engine.Commit()
		ls.mu.Unlock()
		for _, k := range kicks {
			ls.Kick(k)
		}
	})
}

func (ls *Links) sendPending(peer string) {
	ls.mu.Lock()
	lk := ls.Get(peer)
	if lk == nil {
		ls.mu.Unlock()
		return
	}
	if s := lk.Send; s != nil && s.Counter == lk.Counter {
		if s.InFlight || s.Refractory {
			ls.mu.Unlock()
			return
		}
		if s.Timer != nil {
			s.Timer.Stop()
		}
		lk.Send = nil
	}
	promoted := false
	if lk.Pending == nil {
		lk.Pending = ls.NextTx(peer)
		if lk.Pending == nil {
			ls.mu.Unlock()
			return
		}
		promoted = true
	}
	counter := lk.Counter
	lk.Send = &PeerSend{Counter: counter, InFlight: true}
	msg := lk.Pending
	env := SyncRequest{
		From: ls.self, To: peer, Counter: counter,
		Method: msg.Method, Payload: msg.Payload,
		Timestamp: ls.engine.Now(),
	}
	if cr := lk.CachedResponse; cr != nil {
		env.Resp = cr
	}
	if promoted {
		ls.engine.Commit()
	}
	ls.mu.Unlock()

	body, err := ls.engine.Send(peer, env)

	ls.mu.Lock()
	if err != nil {
		log.Printf("[link] %s->%s %s: %v", ls.self, peer, msg.Method, err)
		ls.setRetry(peer, counter, true)
		ls.mu.Unlock()
		return
	}
	var resp SyncResponse
	if json.Unmarshal(body, &resp) != nil {
		ls.setRetry(peer, counter, true)
		ls.mu.Unlock()
		return
	}
	lk = ls.Get(peer)
	if lk == nil || lk.Pending != msg || lk.Counter != counter || resp.Counter != counter {
		if lk != nil {
			lk.Send = nil
		}
		ls.mu.Unlock()
		return
	}

	extraKicks := ls.HandleResponse(peer, msg.Method, msg.Payload, resp.OK, resp.Timestamp)
	lk.AcceptResponse(resp.Timestamp)

	var kickPeer bool
	if ls.Get(peer) != nil {
		lk.Pending = ls.NextTx(peer)
		kickPeer = lk.Pending != nil
	}
	ls.engine.Commit()
	ls.setRetry(peer, counter, false)
	ls.mu.Unlock()
	if kickPeer {
		ls.Kick(peer)
	}
	for _, k := range extraKicks {
		ls.Kick(k)
	}
}

// setRetry sets a per-peer retry timer. Caller must hold Lock.
func (ls *Links) setRetry(peer string, counter uint64, refractory bool) {
	lk := ls.Get(peer)
	if lk == nil {
		return
	}
	if lk.Send != nil && lk.Send.Timer != nil {
		lk.Send.Timer.Stop()
	}
	lk.Send = &PeerSend{
		Counter:    counter,
		Refractory: refractory,
		Timer: time.AfterFunc(ls.engine.RetryDelay, func() {
			ls.mu.Lock()
			if lk := ls.Get(peer); lk != nil {
				lk.Send = nil
			}
			ls.mu.Unlock()
			ls.sendPending(peer)
		}),
	}
}
