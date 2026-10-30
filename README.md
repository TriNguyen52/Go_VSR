# Viewstamped Replication in Go

An implementation of Viewstamped Replication (VSR). The project uses a simple
key-value store to make replication decisions visible, and a deterministic
simulator to control when messages arrive.

The goal is to understand how operations become committed, why a quorum is
needed, and how to test distributed-system behavior reproducibly. Protocol rules
follow *Viewstamped Replication Revisited*. 


## Capabilties

- A key-value store that supports `Put`, `Add`, and `Get`, using text keys and signed 64-bit numbers. Missing keys return zero. Invalid commands or calculations outside the number range return an error and leave the data unchanged.
- One primary and two backups handle one client request at a time. Read requests go through replication just like writes.
- The primary commits a command after one backup confirms it has recorded it. Recording a command in the log does not execute it; execution happens after commitment.
- A lagging backup detects missing log entries and fetches the current primary's full log, even when messages are dropped, duplicated, or reordered.
- A simulator controls time, message delays, and timers. It can cancel timers, limit how many events run, and produce repeatable event records.
- A separate checker verifies that replicas commit the same commands in the same order, execute them only after commitment, and produce the expected results.
- M4 adds stable request numbers, client retry timers, primary retransmission,
  duplicate suppression, and cached replies. Lost requests, prepares,
  acknowledgments, and replies can be retried without repeating an `Add`.

## Run the demo
The module specifies Go **1.27.3**.
From the project root, run:

```sh
go test ./internal/vsr -run 'TestNormalDemo|TestQuorumGate|TestFreshProcessTrace' -count=1 -v
```

The demo sends three commands through the replicated service:

| Command | Result |
|---|---:|
| `Put(x,0)` | 0 |
| `Add(x,1)` | 1 |
| `Get(x)` | 1 |

Replica 0 is the primary. Replica 1 acknowledges immediately, while traffic to
replica 2 is delayed by 100 virtual ticks. The client receives its final reply
at tick 12, before replica 2 has received any operations. Replica 2 catches up
at tick 111; all three finish with `x=1` and three applied operations.

The separate quorum test holds both backup acknowledgments and verifies that
no commitment, application, or reply occurs. Delivering one valid acknowledgment
then completes the request. The determinism test runs the same scenario in two
fresh processes and compares their traces and digests.

Trace columns describe each replica's local progress:

| Column | Meaning |
|---|---|
| `view` | Leadership generation; the M3 demo stays in view zero. |
| `op` | Last operation recorded in the local log. |
| `commit` | Last operation known locally to be committed. |
| `applied` | Last operation executed against the local key-value store. |

A backup may legitimately lag behind. The tests check agreement where committed
history overlaps and require convergence after the workload drains; they do not
require identical logs or snapshots at every instant.



## Crrent architecture

| Package / file | Responsibility |
|---|---|
| `internal/kv` | Executes application commands and provides copied, sorted snapshots. |
| `internal/vsr` | Owns each replica's log and counters. `Step(message)` returns ordered outgoing messages and transition observations. |
| `internal/sim` | Schedules and delivers messages and timers in virtual time. |
| `internal/checker` | Checks observed commitments and application results using an independent reference model. |
| `internal/client` | Keeps one outstanding request, retries its identity, and validates replies. |
| `internal/vsr/vsr_test.go` | Connects the client, three protocol instances, simulator, and checker into executable scenarios. |
| `internal/vsr/retry_test.go` | Drives the real client and replicas with virtual timers and scripted packet loss. |

## M4: retries and duplicate handling

Run the checks and the lost-reply demo:

```sh
go test ./... -count=1
go vet ./...
go test ./internal/vsr -run 'TestRetryLosses|TestLostReplyDemo|TestRetryFreshProcessTrace' -count=1 -v
```

The demo commits `Add(x,1)` as request 1, drops its reply at tick 3, and
retries request 1 at tick 30. The primary returns the cached result at tick 31;
the client receives 1 at tick 32. The primary has applied exactly one operation.
The fresh-process test compares both readable and canonical traces, including
timer generations, and their SHA-256 digest.

Baseline authority is sections 4 and 4.1 of
[Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf):
clients retain request identities on retries, and replicas cache results. The
client retries to all replicas; only the fixed primary processes requests.
The primary still needs only one valid backup acknowledgment in this group
of three. The checker checks committed request identity as well as commands,
and rejects the same request committed at two positions.

**Project choices, not additional baseline rules:**

- There is still one client, with stable endpoint ID 3, and one proposal at a
  time. Request numbers are positive and strictly increase. In replication
  messages the sole client identity is implicit; `RequestID` identifies its
  logical request independently of the log position. Client restart is unsupported.
- Each log entry retains its request number and applied result; an index maps
  requests to positions. Records remain for the entire bounded run, so even
  older completed duplicates return their original cached result. The paper's
  latest-request cache is sufficient for its one-outstanding-request contract;
  retaining all records here also lets us reject conflicting reuse of older IDs.
- Pending duplicates resend the same prepare without resetting its timer.
  A backup re-acknowledges an identical prepare without appending it again.
  Reusing an ID with different command fields returns `REJECT`, a protocol
  error separate from application errors. Equality compares every command field,
  including string bytes and the otherwise ignored value field of `Get`.
- `Step` returns ordered send, observation, arm, and cancel effects. The adapter
  stores the arm effect's local input and delivers it after a positive virtual
  delay. Simulator generations suppress canceled/replaced timers; operation or
  request identity also fences callbacks belonging to earlier work. All effects
  are consumed synchronously before the next input. No core I/O or clock access.
- Test adapters use 10 ticks for primary retries, 30 for client retries, and
  one tick per delivered message. These are educational budgets. Drops are
  scripted at send time and recorded in the readable trace; the simulator is
  unchanged. The loss suite drops the first matching packet on each link,
  forcing retries when both initial prepares or both acknowledgments are lost.
  Separate single-packet tests verify that the other backup permits completion
  without a retry. No-acknowledgment tests hit the event budget with work pending.
- The primary retransmits only while its proposal is uncommitted. M4 alone
  promises retry completion with a responsive primary and quorum. M5 below
  adds gap repair when a later PREPARE or COMMIT reveals missing entries.

All state remains volatile. Crash recovery is unfinished, and simultaneous loss
of all volatile copies is unsupported. View changes, failover, recovery, seeded
campaigns, concurrent clients, and real-process adapters remain deferred.
View-mismatch acknowledgments are rejected, but a genuine old-view scenario
requires M6; M4 stays in view zero. The M3 reliable-delivery fixtures explicitly
ignore timer effects; the M4 adapter executes them.

TigerBeetle comparisons remain pinned to the supplied report's release 0.17.2,
commit `c9b910ad6a8f1c0098a89f3d37c9daf7b1b634c8`. M4 makes no new TigerBeetle
implementation claims. These targeted deterministic tests are not a VOPR campaign.

## M5: gaps and current-view catch-up

```sh
go test ./... -count=1
go vet ./...
go test ./internal/vsr -run 'TestGapDemo|TestTransfer|TestDelayedTransfer|TestGapFreshProcessTrace' -count=1 -v
```

The demo holds operation 1 and its COMMIT to replica 2 for 60 virtual ticks.
Replica 1 lets the primary finish two `Add(x,1)` requests. Replica 2 receives
operation 2 first, stays at op/commit/applied zero, and sends `FETCH_STATE`.
After receiving `STATE`, it installs the full prefix and applies both operations
in order. All replicas finish at applied position 2 with `x=2`. Delayed original
packets and duplicated responses do not repeat an application. The loss tests
drop the first fetch or state response and exercise a virtual retry timer.

**Baseline:** sections 4.1 and 5.2 of
[Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf)
require predecessors before acknowledging a prepare and execution only through
known commitment. Same-view transfer extends the existing history. The paper
uses `GETSTATE` / `NEWSTATE` and can request a suffix from another normal replica.

**Project choices, not additional baseline rules:**

- `FETCH_STATE(view, transferID, knownCommit)` goes only to the current primary.
  `STATE(view, transferID, fullLog, op, commit)` returns request IDs and commands
  in log order, including a pending entry if present. This is the report's
  proposed wire format; no cached application results or KV snapshots are trusted.
- Transfer IDs are positive, increasing per live requester, and separate from
  client IDs. One active transfer keeps its ID across retries. A backup remembers
  the largest missing op and commit observed; repeated gaps do not restart its
  timer (10 virtual ticks in tests). Transfers use the same adapter timer slot as primary proposal retries:
  only backups fetch, so these uses cannot overlap in the fixed view.
- The primary validates membership, view, nonzero ID, and knownCommit. A response
  must match destination, view, current primary, and active ID; its log length
  must equal op, commit must be at most op, and request IDs must be positive and
  strictly increasing. Op and commit must cover both local progress and the
  largest observed gap. Obsolete or malformed responses cause no mutation.
- Every existing entry, including the prepared suffix, must match. A conflict
  asserts before mutation: it indicates a protocol bug under the trusted-message,
  non-Byzantine model. Valid transfers append only missing entries, retain cached
  results and existing KV state, and emit each new commit/apply observation.
  An uncommitted last entry is acknowledged only after the full prefix exists.
- Normal traffic can fill a pending gap. When both target counters are reached,
  the transfer is canceled; late responses and callbacks are ignored. No response
  may decrease op, commit, or applied. Exported transfer slices are freshly copied;
  installation copies their entries. Callers must not mutate an input during Step.
- The test adapter scripts a list of delivery delays per packet: zero copies
  drops it, multiple copies duplicate it, and varying delays reorder it. Each
  copy passes through the simulator's owned-byte boundary. All commit/application
  observations go to the independent checker. KV snapshots are compared only at
  equal applied positions. Fresh processes compare readable and canonical traces.

Catch-up needs a reachable primary and eventual delivery of a retry and response.
An idle backup that loses the final COMMIT without observing a gap can still lag;
there is no idle heartbeat yet. Event-budget exhaustion is reported as incomplete.
Full logs and messages are intentionally unbounded within bounded educational
runs. M5 alone has no crash restart or newer-view installation; M6 adds the latter below.
All state remains volatile; simultaneous loss of all copies is unsupported.

M5 code is in `internal/vsr/transfer.go`, with targeted scenarios in
`internal/vsr/transfer_test.go` and delivery scripts in `retry_test.go`. This
continues the supplied report's testing idea of exercising the real core with
controlled faults and checking internal history. TigerBeetle comparisons remain
pinned to 0.17.2 / `c9b910ad6a8f1c0098a89f3d37c9daf7b1b634c8`; M5 makes no
new claims about that implementation and does not add a random fault campaign.



## M6: explicit view changes

Run the view-change demonstration and fresh-process determinism check:

```sh
go test ./internal/vsr -run 'TestViewChangeDemo|TestViewFreshProcessTrace' -count=1 -v
```

Replica 1 starts without operation 1. A local `SUSPECT(view=1)` at replica 2
starts the real three-message protocol. The first DO_VIEW_CHANGE is dropped;
retries let the new primary obtain the history and preserve the acknowledged
Add. Retrying that request returns 1; a new Add returns 2. Tests also cover
prepared-but-not-committed history, lost START_VIEW, duplicate reports, newer-view
catch-up, suffix replacement, and last-normal-view taking precedence over length.

Baseline authority is section 4.2 of
[Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf).
Reports from two distinct replicas select the largest last-normal-view, breaking
ties by operation number, and the maximum reported commit. Entering view change
fences normal processing. Retained uncommitted history requires new-view PREPARE_OK.
Completed request results survive installation; pending records are rebuilt.

**Project choices, not additional baseline rules:** full logs accompany reports
and START_VIEW. A positive, strictly newer view in a local SUSPECT input is the
scripted trigger. VIEW_RETRY repeats the same view, never automatically advances
it. Timers share the existing adapter slot: entering view change replaces the
proposal/transfer timer, installation cancels it, and pending proposals rearm it.
A normal primary answers view-change retries with its current START_VIEW snapshot.
Newer-primary PREPARE/COMMIT traffic fences the old view and solicits that snapshot;
normal same-view gaps continue to use M5's request-ID-validated transfer. Repeated
START_VIEW in an already installed view is ignored to prevent rollback. Prepared
proposal retries and ordinary gap transfer handle subsequent same-view losses.

Installation asserts agreement with the local committed prefix, preserves its
application/results, and replaces only the speculative suffix. If the new quorum
has not learned an existing local commit, that replica retains its higher commit
and still acknowledges entries the new primary regards as pending. This monotonic
local bookkeeping does not let the new primary bypass its fresh acknowledgment.
The checker observes selection inputs and installations, preserving global
commit history across views, and tests reject fabricated bad observations.

No automatic watchdog, failed-primary escalation, crash/restart, persistence,
random campaigns, or concurrent clients are included. A view can remain stuck
until a further explicit suspicion input. Progress requires eventual delivery
and a functioning designated primary. State remains volatile; simultaneous loss
of all copies is unsupported. TigerBeetle comparisons remain pinned to the report's
0.17.2 / `c9b910ad6a8f1c0098a89f3d37c9daf7b1b634c8`; this milestone adds no
new claims about its implementation.

