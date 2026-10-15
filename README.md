# link

Crash-safe bilateral channel with a shared counter — one decision in flight at a time, so races never arise. Persist before send, persist before respond, retry until delivered. Application modules plug in and write plain request/response logic; link handles ordering, collisions, and crash recovery. Zero external dependencies.

## What it provides

- **Link** — bilateral sync state for one connection: counter, turn bit, pending message, cached response, timestamps
- **Links** — collection of links with policy, module dispatch, and the full sync loop (incoming, outgoing, retry, scheduling)
- **Module** — interface for application modules: HandleRequest, HandleResponse, HandleAccount, NextTx
- **EngineConfig** — callbacks the sync loop needs: Send, Commit, Now
- **Wire types** — SyncRequest, SyncResponse envelopes
- **PeerSend** — runtime send lifecycle tracking (in-flight, refractory, retry timer)

## Usage

```go
import "github.com/jhnngrn/link"

// Engine callbacks — the app provides transport and persistence.
engine := link.EngineConfig{
    Send:   func(peer string, msg link.SyncRequest) ([]byte, error) { ... },
    Commit: func() { /* serialize and persist */ },
    Now:    func() int64 { return time.Now().Unix() },
}

// Create a collection for node "alice"
ls := link.NewLinks("alice", nil, link.InviteModeInvite, engine)

// Register application modules
ls.RegisterModule(myModule)

// Incoming sync — Links handles locking, dispatch, persistence, kicks
resp, err := ls.HandleSync(env)

// Account API — Links handles locking, commit, and kicks internally
result, err := ls.HandleAccount("set_modules", payload)

// Resume pending sends at boot
ls.KickAll()
```

## Design

Links owns the full sync lifecycle: dispatch, locking, persistence (via Commit callback), outgoing sends (via Send callback), retry timers, and scheduling. Application modules register with Links and handle domain-specific protocol messages. The pattern follows kernel TCP — the protocol package owns routing and the loop, modules are like sockets dispatched to.

The sync protocol guarantees:

- **Sequential** — one decision in flight per link, shared counter
- **Crash-safe** — persisted before send, persisted before respond
- **Collision-free** — turn bit determines dominance, alternates each counter
- **Guaranteed delivery** — sender retries until response received

### Modules

A module is anything that implements the `Module` interface:

```go
type Module interface {
    Name() string

    // Incoming request from peer. Return (ok, kicks, err).
    // ok=true accepts, ok=false NACKs. kicks lists peers to send to next.
    HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)

    // Response to our outgoing request. ok reflects the peer's accept/reject.
    // Return kicks.
    HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string

    // Admin/account operation (not sync). Return (response, kicks, err).
    HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error)

    // Pull next outgoing message for peer: (method, payload, ttl).
    // Empty method means nothing to send. Links polls this — the module
    // decides internally what's next: a queue, a state machine, whatever fits.
    NextTx(peer string) (string, json.RawMessage, int64)
}
```

Payloads are `json.RawMessage` — validated but unparsed JSON. The link layer never inspects them; modules parse what they need on their end. Modules don't persist — Links calls `Commit` after every state change.

### Timestamps

The dominant peer (determined by `counter % 2 == turnBit`) sets the authoritative timestamp. Non-dominant senders omit the timestamp; the dominant receiver substitutes its own clock. If the dominant clock is behind the floor (set by the previous dominant peer), it clamps to `max(now, floor)`. Timestamps are validated for monotonicity and a forward cap of 10 minutes.

### Concurrency

Links wraps its state in a `sync.Mutex`. Kick spawns a goroutine for each send attempt. Retry timers use a generation counter — replacing a PeerSend increments the generation, and the timer goroutine checks whether its generation is still current before acting.

## Connection lifecycle

```
  Create(peer)              incoming link.connect
      |                           |
      v                           v
  +---------+              +----------+
  | waiting |              | incoming |
  +----+----+              +-----+----+
       |                         |
  link.connect              AcceptLink
  mutual or ACK             (IntentConnect)
       |                         |
       v                         v
  +-----------+            +-----------+
  | connected | <----------| connected |
  +-----------+            +-----------+
        |
   IntentClose
   (Refs==0)
        |
   link.close ACK
        |
        v
    (deleted)
```

Modules use the same ConnectionState pattern. Close is deferred while Refs > 0 — modules finish their work before the link closes.

## Sync loop

Links runs the outgoing sync loop internally:

- **Kick(peer)** — triggers a send attempt for a peer (async)
- **KickAll()** — kicks all peers, used at boot to resume pending work
- **Schedule(delay, fn)** — runs fn after delay under the Links mutex, commits and kicks any peers fn returns. General-purpose deferred operation — not tied to any specific link or module
- **sendPending(peer)** — acquires the lock, calls NextTx, sends via EngineConfig.Send, handles the response, retries on failure
- **QuickReject(from, method)** — fast pre-auth rejection before expensive signature verification
- **HandleSync(env)** — incoming sync: validates, dispatches to modules, persists, kicks
