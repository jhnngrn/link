# link

Bilateral sync primitive for p2p networks. Zero external dependencies.

Two nodes share a single counter and process one decision at a time. Every message is persisted before it is sent, every response before it is returned. If a node crashes, it picks up where it left off. Two nodes can never disagree about what happened.

## What it provides

- **Link** — bilateral sync state for one connection: counter, turn bit, pending message, cached response, timestamps
- **Links** — collection of links with policy, module dispatch, and the full sync loop (incoming, outgoing, retry, scheduling)
- **ConnectionState** — reusable state machine: Status + IntentConnect/IntentClose + reference counting
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
ls := link.NewLinks("alice", nil, link.InviteModeInvite, supported, engine)

// Register application modules
ls.RegisterModule(myModule)

// Incoming sync — Links handles locking, dispatch, persistence, kicks
resp, err := ls.HandleSync(env)

// Account API — Links dispatches, commits, and kicks internally
ls.Lock()
result, err := ls.HandleAccount("set_modules", payload)
if err == nil { ls.Commit() }
ls.Unlock()

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
- **Schedule(delay, fn)** — runs fn after delay under the Links mutex, kicks any peers fn returns
- **sendPending(peer)** — acquires the lock, calls NextTx, sends via EngineConfig.Send, handles the response, retries on failure
- **QuickReject(from, method)** — fast pre-auth rejection before expensive signature verification
- **HandleSync(env)** — incoming sync: validates, dispatches to modules, persists, kicks
