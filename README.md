# creditnet

Federated bilateral credit network. Each server hosts users identified as `alice@example.com`. Each user is a node in the network with its own links and balances. Servers communicate peer-to-peer over HTTP with Ed25519 signatures. Core handles links, sync transport, credit limits, demurrage, receipts, and single-hop payments but without any credit clearing. Modules add payment routing (Ripple, Resilience), loop clearing (Loops), hierarchical tree payments (Hierarchy) and triangular debt clearing by novation (Triad) on top.

Responsibility is divided along two axes — trust scope and operation type:

```
                  Payment              Clearing   Redistribution
  Single-hop      core                 triad      demurrage (core)
  Multi-hop       ripple/hierarchy     loops      resilience
```

Single-hop: only between direct neighbours. Multi-hop: routed through intermediaries. Payment: moves value from A to B. Clearing: reduces existing debt without new value transfer.

## Build

```
go mod tidy
go build ./cmd/creditnet    # all modules
go build ./cmd/ripple       # ripple only
go build ./cmd/resilience   # resilience only
go build ./cmd/loops        # loops only
go build ./cmd/hierarchy # hierarchy only
go build ./cmd/triad     # triad only
go build ./cmd/base         # bare barter, no modules
```

## Run

```
./creditnet -domain node1.example.com -port 3000 -scheme http
```

Flags:

- `-domain` — server identity (required, or `DOMAIN` env)
- `-port` — HTTP port (default 3000)
- `-data` — data directory (default `/var/lib/creditnet`)
- `-scheme` — peer communication scheme, `https` or `http` (default `https`)

All binaries share the same data directory. Switch binary without migrating data.

## Architecture

```
core/                          neutral bilateral substrate
  server.go                      server, config, node lifecycle, module registry
  handlers.go                    barter: bandwidth, receipts, trustline, payment, transfer
  sync.go                        bilateral sync protocol
  demurrage.go                   demurrage: AddBalance, EffectiveBalance, mulDiv
  message.go                     fire-and-forget transport
  account.go                     user API dispatch
  types.go                       wire types, link, module interface, callbacks
  keys.go                        Ed25519 signing and verification
  users.go                       auth, sessions, admin, argon2 password hashing
  store.go                       persistence (file-based)
  run.go                         Boot/RunAndWait helpers

jlib/                          Engine: pathfinder + settlement in one struct
  jlib.go                        Engine struct, constructor, DrainKicks, OnRemoveLink, Reserved
  pathfinder.go                  search/recurse/found/prepare, flood/climb, ping-pong
  settlement.go                  3PC: commit/seal/finalize/cancel/cleanup/rollback

ripple/                        flood search (imports jlib)
  app.go

resilience/                    flood search + fee redistribution (imports jlib)
  app.go

hierarchy/                 climb search (imports jlib)
  app.go

loops/                         bidirectional flood (standalone 2PC + chunking, imports only core)
  app.go

triad/                      bilateral 2PC (imports only core)
  app.go

cmd/
  base/                          bare barter — core only
  ripple/                        core + ripple
  resilience/                    core + resilience
  loops/                         core + loops
  hierarchy/                 core + hierarchy
  triad/                      core + triad
  creditnet/                     core + all modules
```

## Core

Core provides a neutral bilateral substrate: links, sync, balances, demurrage. It knows nothing about multi-hop payments or pathfinding. Anyone can build any protocol on top — 2PC, 3PC, or anything else.

### Sync protocol

Crash-safe guaranteed delivery. Each side persists what it will send before sending, and persists the response before responding. The sender retries until it gets a response. If either side crashes and restarts, it picks up where it left off. No operation is lost, no operation is applied twice.

One decision in flight per link. Decisions between peers happen strictly sequentially — both sides share a single counter. If both propose a decision at the same time, one peer is dominant and the other yields. Dominance alternates with each counter value: one peer is dominant on even, the other on odd. Neither side monopolizes the link.

These rules guarantee that two peers that have a link are always in agreement on the sequence of decisions, without any misunderstanding. They always reach the exact same state. The sync protocol is the fundamental building-block of the Creditnet framework, it solves a hard problem and frees module engineers from having to solve it manually.

### Core sync methods

`sync_trustline` — propose credit limit. Peer ACKs to accept. Both sides store their own outgoing limit.

`sync_demurrage` — propose demurrage rate for the link.

`payment` — balance change with receipt. Used for direct bilateral payments. Always ACK'd if bandwidth allows.

`transfer` — balance change without receipt. Used by modules for gas compensation. Core accumulates transfers in `transferOutbox` and syncs them. Amounts below `TransferThreshold` (256) are deferred.

`sync_module` — bilateral module activation/deactivation. Payload: `{module, active}`. Activation uses a transient outbox: `set_modules` sets `false` (pending) and queues a one-shot proposal. ACK → `true`. NACK → outbox entry consumed, `false` stays — intent preserved without re-proposal loop. Peer can still propose to the `false` side and get ACK'd → convergence regardless of order. On boot, all `false` modules are re-queued for one proposal attempt. Deactivation: sender notifies peer it removed a module → peer deletes its entry → ACK.

### Messages

Fire-and-forget delivery alongside sync. No ACK, no queue, no ordering guarantee. Used for pathfinding probes, search floods, and other discovery traffic. Module's `HandleMessage` processes them. Per-link module filtering applies.



Core offers `TransferFunc` as a callback — a non-receipted balance transfer synced via `transfer`. Modules use it for gas compensation without core knowing about gas.

## jlib — multi-hop payment library

`jlib/` provides a single `Engine` struct combining pathfinding and settlement. One struct, two files: `pathfinder.go` handles search, `settlement.go` handles 3PC. Modules import it as a Go package. Core knows nothing about jlib.

```go
srv.Register("ripple", ripple.New, ...)
srv.Register("hierarchy", cc.New, ...)
srv.Register("triad", triad.New, ...)
```

Ripple, Resilience and Hierarchy import jlib for 3PC settlement and pathfinding. Loops and Triad are standalone — they import only core. Gas compensation uses core's `TransferFunc`: each module calls `transferFn(peer, amount)` and core syncs via `transfer`.

### Settlement — 3PC with penalty asymmetry

Hash chain: `seller_preimage → hash → cancel_preimage → hash → payment_id`. Seller creates the preimage. Cancel reveals `cancel_preimage`. Finalize reveals `seller_preimage` (cryptographic receipt).

Each side tracks `CommitTime`, `SealTime`, `Fee`, and `Penalty` independently. The penalty math creates asymmetric pressure at seal:

1. **Commit** propagates forward (buyer → seller). Each hop reserves amount + fee + penalty. Penalty ticks via finish-on-timeout — if nobody acts, the full amount pays out.
2. **Seal** propagates forward from buyer. Buyer gives up the right to cancel. Seal snapshots the penalty phase: `amountFinal = Amount - (ticker - phase1)` where `phase1` is ticks before seal. After seal, cleanup only pays out the pre-seal portion (cancel-on-timeout effect).
3. **Finalize** propagates backward from seller with `seller_preimage`. Settles `amountFinal + proportional fee + proportional penalty`.

The pressure: a hop that received seal but hasn't forwarded it has IN-side sealed (cleanup = partial) and OUT-side unsealed (cleanup = full). The difference forces propagation.

Cancel before seal pays proportionally: `(amount + fee + penalty) * elapsed / amount`. Cancel after seal is rejected.

Pathfinder and settlement share one `Engine` struct. `ReqCommit` reads `Paths` directly, computes fees, creates the Payment, and sends `counterpart_seal` — no parameter extraction in app.go.



### Pathfinder — shared discovery

Ripple and Hierarchy share the same pathfinder. A `Linear` flag selects flood (Ripple) or climb (Hierarchy). Modules provide topology callbacks:

- `PeersFn` — flood: returns all linked peers
- `ParentFn` — climb: returns parent ("" = root → bollplank)
- `GasPerRound(depth)` — exponential or flat budget

Flood mode: search fans out to all peers, depth_ack gates each round. Climb mode: search goes to parent, root bounces recurse back (bollplank), no depth_ack.

Payment flow: buyer sends `payment_request` → seller accepts, creates hash chain, sends `payment_accept` with `cancel_preimage` → buyer derives ID, starts search → ping-pong alternation → collision/LCA → `found` → `prepare` → `counterpart_commit` → buyer starts 3PC via Settlement.

There is no cheapest-path routing. The buyer proposes a fee rate (per hop) in the search. Each intermediary checks whether the offered fee meets its own minimum (`FeeRate / 2`) and silently drops the search if it doesn't. The path that forms is the first one where every hop found the fee acceptable — not the cheapest possible route.

### Gas

Gas compensates peers for search work. Sender pays receiver via `transferFn(peer, amount)`. Core accumulates in `transferOutbox` and syncs via `transfer` above threshold (256 units).

Gas scales with topology. Ripple: `16^(depth+1)` per round (exponential, flood). Hierarchy: `1024` per hop (flat, climb — each intermediary takes a fixed charge). Loops: `16^(depth+1)` per round (exponential, bidirectional flood).

## Modules

### Ripple — payment routing

Multi-hop payments via flood search. Search floods to all linked peers with gas budget, depth_ack gating, iterative deepening. Fee and penalty per hop. The seller endpoint does not receive the per-hop fee — only intermediaries earn fees. Fee rate is per-node, settable via `set_fee_rate`, default 1%.

### Resilience — payment routing with fee redistribution

Ripple with two additions: width tracking and fee redistribution. After settlement, earned fees are redistributed to peers with positive balance, weighted by each peer's width — a running average of the tax rate on payments that moved the balance away from zero. Redistribution cascades: when a peer receives a redistribution that reduces its negative balance, it redistributes onward to its own peers, creating a swarm effect that spreads fee income through the network with diminishing amounts.

Unlike Ripple and Hierarchy, the seller endpoint receives the per-hop fee (SellerFee). This is appropriate because the fee is redistributed anyway — it passes through the seller and into the network.

Two sync methods beyond jlib's settlement: `direct_payment` (bilateral payment with tax and redistribute, like C Resilience's `direct_payment`), `redistribution` (swarm cascade, no new tax, like C's `direct_transfer`). Account method `local_payment` initiates a direct payment. Fee rate is per-node, settable via `set_fee_rate`, default 1%. Width and fee rate are persisted as module state. The tax buffer (pending redistribution amounts) is transient — if the node restarts, unsent redistributions are lost.

### Hierarchy — hierarchical tree payments

Payments through a tree hierarchy. Search climbs to parent; root (no parent) bounces recurse back as bollplank. Each node configures its parent via `set_parent`. Bilateral LCA discovery via ping-pong. Gas per hop is 1024 (flat). MaxDepth 8.

### Loops — loop clearing

Discovers and clears debt cycles via bidirectional flood. Standalone module — imports only core, not jlib. Own 2PC settlement with per-hop penalty and chunking. Penalty per hop, same model as settlement: IN = `penalty*(hops+1)`, OUT = `penalty*hops`. Initiator pays, intermediaries earn. Hash chain `seed → h1 → ... → hN`: finalize peels 1 layer, cancel peels 2. Intermediary seeing finalize preimage cannot cancel (needs one layer deeper, which only the initiator has). First chunk uses commit around the loop. Subsequent chunks: finalize IS the transition — each hop settles and creates a new Payment with the same peers. Initiator queues next finalize directly. No further commits. Last chunk marked `final` (chain exhausted or bandwidth depleted). Clears up to 1023 chunks (~$1023) per search. No seal — initiator controls both ends.

### Triad — triangular debt clearing

Restructures triangular debt. 2-phase commit/finalize between three known parties. No pathfinding, no penalty, no gas. Imports only core.

## Per-link modules

Each link can restrict which modules are active:

```
add_link {peer: "rep@region.org", modules: ["hierarchy"]}
add_link {peer: "bob@local.lets", modules: ["triad", "loops"]}
add_link {peer: "alice@other.net", modules: ["ripple"]}
add_link {peer: "carol@swarm.net", modules: ["resilience"]}
```

Omitting `modules` enables all registered modules (no restrictions). When modules are specified, they start as `false` (pending) and activate bilaterally via `sync_module`. A one-shot proposal is queued; if NACKed, `false` stays — intent preserved, no re-proposal loop. The peer can propose later and the `false` side ACKs. On boot, all `false` modules get one fresh proposal attempt. Removing a module from the list deactivates it locally and notifies the peer. Filtering applies to all three dispatch paths: sync methods error (HTTP 400 — counter stays, sender retries), messages are dropped, and NextTx skips disabled modules. Messages from peers without a link (counterpart messages) are always delivered.

## Sync protocol

Bilateral, persistent, one-at-a-time per link. Timestamps in seconds. A shared counter guarantees ordering. TurnBit breaks collisions.

Each link has a single TX slot. Promote order: core outboxes first, then modules in registration order, then transfer last.

`HandleRequest` returns `(true, nil)` ACK, `(false, nil)` NACK, or `(false, error)` error. Error → HTTP 400, counter stays, sender retries, `HandleResponse` never called. NACK → counter advances, `HandleResponse` called with `ok=false`. If a `Resp` does not check `ok`, its `Req` must never NACK — the sender would apply side effects unconditionally, diverging from the receiver.

A sync link is treated as a single machine split across two processes. NACK is a decision within the shared instruction set — "I understood, the answer is no." Error means the two sides disagree on something they should agree on. Two categories: ISA mismatch (disabled module, unknown method, malformed payload) and persistent state mismatch (a committed payment exists on one side but not the other). Both are bugs, not decisions. The retry forces the inconsistency to surface rather than letting state silently diverge. Temporary state mismatches — a path expired before commit arrived — are legitimate and produce NACK, not error, because temporary state can differ without violating the shared-machine invariant. The channel should work as reliably as hardware within the same machine; errors that persist indicate a mismatch that must be resolved, not tolerated.

## Demurrage

Rate in promille per year. Creditor's rate applies to peer's debt. Computed on-the-fly, materialized at shared sync timestamp.

## Method namespacing

Core prefixes methods with the registered module name. `"commit"` registered under `"ripple"` → wire `"ripple.commit"`. Modules never see the prefix.

## Interoperability

Nodes running different binaries interoperate as long as they agree on modules via `sync_module`. Unknown sync methods error (HTTP 400 — counter stays, sender retries) since bilateral module negotiation means an unknown method is a protocol violation, not a valid request to reject.

## API

All endpoints accept JSON `{method, payload}`.

**POST /api/auth** — `register`, `login`, `logout`, `change_password`

**POST /api/admin** — `create_user`, `delete_user`, `list_users`, `reset_password`

**POST /api/account** — requires bearer token:

Core: `add_link` (`{peer, credit_limit?, modules?}`), `remove_link`, `set_credit_limit`, `set_demurrage`, `set_modules` (`{peer, modules}`), `payment`, `status`, `pending_links`, `dismiss_link`

Ripple: `ripple.send_payment` (`{recipient, amount}`), `ripple.accept_payment`, `ripple.pending_requests`, `ripple.set_fee_rate` (`{rate}` — promille, default 10 = 1%)

Resilience: `resilience.send_payment`, `resilience.accept_payment`, `resilience.pending_requests`, `resilience.local_payment` (`{peer, amount}`), `resilience.set_fee_rate` (`{rate}`)

Hierarchy: `hierarchy.send_payment` (`{target, amount}`), `hierarchy.accept_payment`, `hierarchy.pending_requests`, `hierarchy.set_parent`, `hierarchy.set_fee_rate` (`{rate}`)

**POST /p2p/tx** — bilateral sync. Wire: `ripple.commit`, `resilience.commit`, `resilience.direct_payment`, `resilience.redistribution`, `hierarchy.search`, `triad.commit`, `triad.finalize`, `transfer`, etc.

**POST /p2p/msg** — fire-and-forget. Wire: `ripple.search`, `hierarchy.search`, etc.

**GET /p2p/key** — public key for signature verification

## Web UI

`ui.html` is a single self-contained file — no build step, no dependencies. The UI displays 100000 base units as 1 — gas costs scale well at this granularity.

Features: login/register, link management with inline credit limit/pay/demurrage controls, pending link requests with expiry countdown, receipts, per-link module config, send payment (Ripple, Hierarchy or Resilience), fee rate setting, admin panel (create/delete users, reset passwords), change password. Dark/light mode.
