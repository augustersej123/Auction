# Report notes – Distributed Auction

Working notes to build the report from. Organised by the four required report sections.
Status: **replication done and tested; forwarding, failover and client retry still to do.**

---

## Progress log

| # | Step | Status |
|---|------|--------|
| 0 | Scaffolding: proto, server node, client (Go + gRPC) | ✅ |
| 1 | Primary replicates full state to backups on every bid | ✅ |
| 2 | Primary replies `SUCCESS` only after replication | ✅ |
| 3 | Backup copies the bidders map in `Replicate` | ✅ |
| 4 | Crash test: bid on node 0, kill node 0, `result` on node 1 shows the bid | ✅ passed |
| 5 | Backups don't accept bids themselves (forward to primary / reject) | ⬜ |
| 6 | Failure detection with `Heartbeat` + backup takes over as primary | ⬜ |
| 7 | Client retries the next node on failure | ⬜ |
| 8 | Full crash test without manual steps | ⬜ |

---

## 1. Introduction (material)

- A replicated auction service in Go using gRPC.
- Uses **passive replication (primary–backup)**: one primary handles bids, backups hold copies of the state.
- Each node is a **separate process** (not a thread), started with its own id, port and list of peers.
- Clients can talk to any node they know about (`-nodes` flag).
- Tolerates **one crash-stop failure**, i.e. a node that stops and never comes back.
- API: `bid(amount) → SUCCESS | FAIL | EXCEPTION`, `result() → winner if over, else highest bid`.
- We added a `bidder` name to `Bid`, because the system must know who is bidding in order to register bidders and name a winner.

---

## 2. Architecture (material)

### Components
- `proto/auction.proto` – one gRPC service with two groups of calls:
  - **Client-facing:** `Bid`, `Result`.
  - **Internal (node ↔ node):** `Replicate`, `Heartbeat`.
- `server/` – an auction node. Node `0` starts as primary.
- `client/` – interactive bidder (`bid <amount>`, `result`, `quit`).

### State held by every node
| Field | Meaning |
|-------|---------|
| `highestBid`, `highestBidder` | Current leader of the auction |
| `bidders` | Every registered bidder → their highest bid |
| `endTime` | When the auction closes (default 100 s after start) |
| `seq` | Version number, incremented on every accepted bid |
| `isPrimary` | Whether this node is in charge |

### Bid protocol (primary)
1. Lock the state, so bids are processed one at a time.
2. Reject with `FAIL` if the auction is over or the bid is not higher than the current highest.
3. Apply the bid locally. A first bid registers the bidder. Increment `seq`.
4. **Send the full state to every backup with `Replicate`** and wait for each answer (timeout 1 s per backup).
5. Only then reply `SUCCESS` to the client.

### Replicate protocol (backup)
- If `req.seq > own seq`: overwrite the local state with the received state (with its own copy of the map).
- Otherwise: ignore it, so an old or duplicate message can never roll the state back.
- Always answer `ok`.

### Result
- Reads the local state: if the time is past `endTime`, report the winner; otherwise report the current highest bid.

### Design choices and why
- **Reply after replicating** – a client never gets `SUCCESS` for a bid that a surviving node doesn't know about.
- **Send the full state, not deltas** – simpler; a backup that missed an update catches up on the next one. The state is tiny.
- **Timeout = crashed** – allowed by the assignment's assumption: messages to non-failed nodes arrive within a known time limit.
- **Lock held during replication** – bids are totally ordered by the primary; one bid finishes (including replication) before the next starts.
- **`endTime` is replicated** – a backup that takes over closes the auction at the same time.

### Still to design / implement (for this section later)
- Backups forward `Bid` to the primary (or reject it).
- `Heartbeat`: backups ping the primary periodically. If no answer arrives within the time limit, the backup with the lowest id becomes primary.
- Client: on error, switch to the next address and retry.

---

## 3. Correctness 1 – linearisability vs. sequential consistency

### Definitions (to state precisely in the report)
- **Linearisability:** every operation appears to take effect atomically at a single point in time between its invocation and its response. The resulting order is a legal sequential execution that **respects real-time order**: if op A finishes before op B starts, A comes before B.
- **Sequential consistency:** the result is the same as some legal sequential execution in which each client's operations appear **in program order**, but operations from *different* clients need **not** respect real-time order.
- Linearisability ⇒ sequential consistency, but not the other way around.

### Our argument (draft): we aim for linearisability
- All bids go through one primary, which processes them one at a time under a lock. This gives a single total order of bids (`seq`).
- **Linearisation point of a bid:** when the primary applies it under the lock. The primary has replicated it to all live backups before responding, so every node that can answer afterwards already has it.
- So when a client gets `SUCCESS`, every later `result` from any live node reflects that bid. Real-time order is respected.
- **Caveats to handle or mention:**
  - Until step 5 is done, a backup can accept bids itself. That breaks the single order, so step 5 is needed for the argument.
  - `result` on a backup *during* an in-flight bid may or may not see the bid. That's fine, because the two operations overlap in time and either order is allowed.
  - Before the first bid, each node's `endTime` comes from its own start time, so nodes started at different times disagree slightly until the first replication. Options: mention it, or pass an absolute end time when starting the nodes.

---

## 4. Correctness 2 – with and without failures

### No failures
- One primary orders every bid; all backups receive every state change, in `seq` order (ordered, reliable transport).
- All nodes therefore hold identical state after each acknowledged bid. `result` is the same everywhere.
- Rules hold: bids must exceed the current highest; the first bid registers the bidder; after `endTime` no bids are accepted and the highest bidder is the winner.

### Backup crashes
- The primary's `Replicate` to it times out. Because of the known time limit, the primary treats it as crashed and carries on. Clients are unaffected.

### Primary crashes (tested in step 4)
- Every bid that received `SUCCESS` was replicated first, so the backup has it. **No acknowledged bid is lost.**
- With failover (step 6), the backup detects the crash with `Heartbeat` and becomes primary, continuing from the same state and the same `endTime`.
- With client retry (step 7), clients transparently continue against the new primary.

### Edge case: primary crashes in the middle of a bid
- The client gets no reply (→ `EXCEPTION`). The bid may or may not have reached the backup.
- Either outcome is consistent: the client never saw `SUCCESS`, so neither "it happened" nor "it didn't" contradicts anything a client observed.
- If the client retries the same amount and the bid did get through, the retry returns `FAIL` (not higher than the current highest), which is consistent with the state.

### Why one failure is enough with 2+ nodes
- Crash-stop: f + 1 replicas tolerate f crashes, so 2 nodes tolerate 1. Using 3 gives margin.
- Crash detection is reliable thanks to the timing assumption (synchronous system), so we never have two primaries at once.

---

## Testing log
- **Step 4 crash test (passed):** node 0 (primary) and node 1 started; `bid 10` as alice on node 0 → `SUCCESS`; node 0 killed with Ctrl-C; `result` on node 1 → `Highest bid: 10 by alice`.
