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
- A delayed backup catches up once its messages arrive, provided none are lost and they arrive in order.
- A simulator controls time, message delays, and timers. It can cancel timers, limit how many events run, and produce repeatable event records.
- A separate checker verifies that replicas commit the same commands in the same order, execute them only after commitment, and produce the expected results.

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
| `view` | Leadership generation; currently always zero. |
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
| `internal/vsr/vsr_test.go` | Connects the client, three protocol instances, simulator, and checker into executable scenarios. |


