# link

Bilateral sync primitive for p2p networks. Zero external dependencies.

Two nodes share a single counter and process one decision at a time. Every message is persisted before it is sent, every response before it is returned. If a node crashes, it picks up where it left off. Two nodes can never disagree about what happened.

## What it provides

- **Link** — bilateral sync state for one connection: counter, turn bit, pending message, cached response, timestamps
- **Links** — collection of links with policy (InviteMode, Supported modules, MaxLinks)
- **ConnectionState** — reusable state machine: Status + IntentConnect/IntentClose + reference counting
- **Protocol handling** — HandleRequest, HandleResponse, NextTx, ValidateRequest, AcceptRequest, AcceptResponse
- **Module negotiation** — propose, accept, reject modules on a link
- **Account operations** — HandleAccount dispatches set_invite_mode, set_modules, accept_module, reject_module, status
- **Wire types** — SyncRequest, SyncResponse envelopes
- **PeerSend** — runtime send lifecycle tracking (in-flight, refractory, retry timer)

## Usage

```go
import "github.com/jhnngrn/link"

// Create a collection for node "alice"
links := link.NewLinks("alice", nil, link.InviteModeInvite, supported)

// Create a link to "bob"
lk := links.Create("bob")
lk.State.IntentConnect = true

// Get next message to send
msg := links.NextTx("bob")

// Handle incoming request
ok, del, err := links.HandleRequest("bob", method, payload)

// Handle response
del := links.HandleResponse("bob", method, payload, ok)

// Account operations (returns result + peers to kick)
result, kicks, err := links.HandleAccount("set_modules", payload)
```

## Design

All dispatch goes through `Links` with peer as parameter — the same interface pattern as application modules. The caller provides transport, locking, persistence and kick logic. The link package never calls back into the caller.

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
  +-----+-----+            +-----------+
        |
   RemoveLink
   (IntentClose, Refs==0)
        |
   link.close ACK
        |
        v
    (deleted)
```

Modules use the same ConnectionState pattern. Close is deferred while Refs > 0 — modules finish their work before the link closes.
