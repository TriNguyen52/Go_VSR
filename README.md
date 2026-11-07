# Viewstamped Replication in Go

This project implements **Viewstamped Replication (VSR)** through a key-value store supporting `Put` (set a value), `Add` (increase or decrease it), and `Get` (read it). A missing key reads as zero. Adding beyond the integer's limits returns an error without changing the data.

## Try it

The repository uses Go **1.27.3**. From the repository root, run:

```powershell
go test ./...
```

Run these demonstrations with verbose output to see the replicas' progress:

```powershell
# One Add request, with no network faults.
go test ./internal/vsr -run '^TestNetworkLab$/baseline$' -count=1 -v

# Lose the reply, stop the primary, and retry without adding twice.
go test ./internal/vsr -run '^TestAutomaticFailoverAmbiguousOutcome$' -count=1 -v

# Separate the primary from the other replicas, then restore communication.
go test ./internal/vsr -run '^TestNetworkLab$/partition-heal$' -count=1 -v
```

The first demonstration ends with `x=1` on all three copies. The second shows that a retried `Add(x,1)` returns 1, and a new, separate `Add(x,1)` returns 2. The third shows the two connected copies continuing, then the old primary catching up after communication returns.

For more experiments, see the [network fault lab](vsr-network-lab.md). 


## What currently works

| Situation | Implemented behavior |
|---|---|
| Normal reads and writes | Three replicas record requests in order; the primary waits for a quorum before executing and replying. |
| Lost requests, acknowledgments, or replies | The client and replicas retry. Repeating the same request number returns the saved result instead of executing it again. |
| Duplicate or out-of-order messages | Duplicate messages do not repeat an operation. A backup with missing entries fetches history before acknowledging the later entry. |
| A slow or disconnected backup | The other two replicas can continue. The lagging backup catch up later. |
| A primary that stops or becomes unreachable | Local timeouts start a change of primary. The connected pair can resume service, preserving committed work. |
| The next primary is also unreachable | Further timeouts move to later views until a suitable primary and quorum can communicate. |
| No communicating quorum | New requests remain unfinished. |

A missing reply does **not** mean a request failed. For example, an `Add` may
commit just before the primary stops. The client keeps the same request number
when retrying, allowing the new primary to return the existing result.

## Assumptions and current limits

The current failures are stopped replicas and messages that are delayed, lost, duplicated, or reordered. Sender identities are trusted. Corrupted data are not considered.


**Crash recovery is implemented (M8).** A restarted replica begins empty and cannot vote until both survivors respond, including the primary of the highest reported view. At most one replica may be crashed or recovering at a time. Losing all volatile copies remains unsupported; there is no disk persistence. The single client stays alive and keeps increasing request numbers.

## Recovery demonstration

```powershell
go test ./internal/vsr -run '^TestRecoveryDemo$' -count=1 -v
```

![Recovery protocol sequence](docs/recovery.svg)

See [recovery protocol details](docs/recovery.md) for nonce handling, timer fencing, and the failure budget.

## Files

| Location | Purpose |
|---|---|
| [`internal/kv`](internal/kv) | The integer key-value store and command behavior. |
| [`internal/vsr`](internal/vsr) | Replication, catch-up, view changes, local timeouts, and scenario tests. |
| [`internal/client`](internal/client) | Request numbering, retries, and reply handling. |
| [`internal/sim`](internal/sim) | Virtual-time event delivery, timers, and traces. |
| [`internal/checker`](internal/checker) | Independent checks of history and application results. |


