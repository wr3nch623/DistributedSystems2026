# DS1 Project — 20-Day Execution Plan (Java + Akka)

Source: `ds1_project_2026_v3.20260625.pdf`
Mandatory starter: https://github.com/StefanoGenettiUniTN/ds-student-project-2026

---

## 0. What is non-negotiable

- [ ] Java + Akka actors; each replica and client is an actor with a unique ID.
- [ ] Use the starter codebase as-is; `Client` and `Replica` must extend the provided abstract classes.
- [ ] Use `AbstractReplica.tell(...)` for all replica-to-replica messages (emulated delay + FIFO).
- [ ] Never print directly. Use `Logger.log/debug`; tests can fail because of stdout.
- [ ] All base tests must pass: `APICompliance`, `NoCrashes`, `WithCrashes`.
- [ ] Logs use the exact formats and timestamps from the spec.
- [ ] Read requests are answered locally and immediately by the contacted replica.
- [ ] Writes go through the current coordinator using UPDATE → ACK → quorum → WRITEOK.
- [ ] Update ID is `<epoch>:<sequence>`; epoch changes on election; sequence resets to 0.
- [ ] Replicas keep update history for recovery.
- [ ] Heartbeats every `getCoordinatorBeatInterval()` ms.
- [ ] Ring election; skip crashed next node after ACK timeout.
- [ ] New coordinator broadcasts SYNCHRONIZATION and catches up all replicas before serving writes.
- [ ] Interrupted updates that reached a quorum must never be lost.
- [ ] Crash injection via `AbstractReplica.Crash` must work at: Now, Heartbeat, Update, WriteOK, Election.
- [ ] Report: 3–4 pages target, 6 max, English, provided LaTeX template, mandatory LLM-use section.
- [ ] Presentation: 12 minutes + questions; 3–4 representative executions, including corner cases.
- [ ] Project is for exactly two students. If you are solo, contact the instructor immediately.
- [ ] Submit code + report as `Surnames.tgz` using `tar -czvf`; book the slot by email before the deadline.
- [ ] Book the presentation slot in the first 48 hours, not after the code is finished. Slots fill up.
- [ ] Download the official LaTeX template and the presentation slides on Day 1. Do not invent a format.
- [ ] Start an LLM-usage log on Day 1; the report cannot be presented without the mandatory section.
- [ ] Both teammates must understand every part of the code; grading is individual.
- [ ] Do not implement features beyond the specification. Extra features cost time and add failure modes.
- [ ] Use Gradle 9.2.1 and a compatible JDK (JDK 17 is the safest choice).
- [ ] Do not modify the abstract base classes unless a real bug forces it; the tests depend on their behavior.

---

## 0.0 The 15 things most likely to cost you the project

1. **Not using `createBaseReceiveBuilder()`** in both `Client` and `Replica` → tests cannot inject requests/init/crash.
2. **Extra or missing callbacks** → probes fail even when the protocol works.
3. **Duplicate `UpdateApplied`** from apply + sync + duplicate WRITEOK → exactly-once failure.
4. **Only storing applied updates in history** → quorum-acked update lost after coordinator crash.
5. **Not skipping crashed nodes in the ring** → election never completes when a majority is exactly alive.
6. **Not retrying/queueing writes during election** → `WithCrashes` client timeout.
7. **Using direct `ActorRef.tell` instead of `AbstractReplica.tell`** → no emulated delay/FIFO, timing failures.
8. **Printing to stdout** → tests fail because of output/timing interference.
9. **False-positive crash detection** → elections during `NoCrashes`, extra callbacks, flaky tests.
10. **Split election / two winners** → probes disagree on `CoordinatorElected`.
11. **Replying with the coordinator's ID instead of the contacted replica's ID** → `fromReplica` mismatch.
12. **Late success callback after a client timeout** → unexpected probe message.
13. **Not stopping sends after crash** → crashed replica still ACKs/forwards, violating crash semantics.
14. **Pipelining multiple updates before the basic path is green** → ordering/dedup bugs.
15. **Leaving the report/submission to the last two days** → page limit, LLM section, slot booking, archive mistakes.

---

## 0.1 Critical audit — the things that actually decide pass/fail

Read this before writing protocol code. Most lost marks come from these, not from the core algorithm.

### A. Test-contract mistakes that fail even when the protocol is correct

- [ ] `Client.createReceive()` **must** start from `createBaseReceiveBuilder()`. Otherwise test-injected `ReadRequest`/`WriteRequest` messages are never handled.
- [ ] `Replica.createReceive()` **must** start from `createBaseReceiveBuilder()`. Otherwise `InitSystem` and `Crash` are never handled.
- [ ] Call **only** the provided callbacks; never send custom messages to the test listener/probe. Tests use `expectMsgClass`/`expectMsgEquals`, so one extra callback arriving first can fail a test.
- [ ] `callbackOnUpdateApplied(index, value)` exactly **once per replica per committed update**. Deduplicate by `UpdateId`; retries, duplicate WRITEOK, and synchronization must not apply/callback twice.
- [ ] `callbackOnElectionStarted(crashedCoordId)` exactly **once per replica per election**. Do not call it every time an ELECTION is forwarded or duplicated.
- [ ] `callbackOnCoordinatorElected(newCoordId)` exactly **once per replica per election/epoch**. The winner calls it when it decides it won; every other replica calls it when it processes SYNCHRONIZATION. Do not call it again for a duplicate sync.
- [ ] Never call any callback after the replica is crashed. The crashed actor must be silent.
- [ ] `ReadResult` and `WriteResult` must use the **contacted replica's ID** as `fromReplica`, not the coordinator's ID.
- [ ] Reads must be answered **locally and immediately** by the contacted replica; do not ask the coordinator.
- [ ] On timeout, call `callbackOnReadTimeout`/`callbackOnWriteTimeout` with the exact `client`, `replica`, `index`, and `value`. Do not also send a success callback if a late reply arrives.
- [ ] Keep a per-request pending map in the client. Late/unknown responses are ignored.
- [ ] The abstract crash wrapper already logs and sends the `Crash` object to the probe when it receives a `Crash` message. Do not send a second `Crash` message yourself.
- [ ] Do not call `callbackOnElectionStarted` on a replica that merely learns about an election but never joins it. In the ring, every participant joins when the message reaches it.
- [ ] Avoid probe-visible noise: no custom callbacks, no `UpdateApplied` for reads, no `CoordinatorElected` before election, no duplicate `UpdateApplied`.
- [ ] Test probes are per-replica. A replica's probe must receive only its own callbacks; never forward another replica's callback.

### B. Timing budget — the numbers that matter

Given `maxLatency = M`, `N = number of replicas`, `beat = coordinatorBeatInterval`:

- [ ] `getMaxUpdateDelay ≈ 6M + N + 200` ms. For `N=22, M=20`: about **342 ms** for one write to complete from the client's perspective.
- [ ] `getClientReadTimeout = M * N * 8`. For `N=22`: **3520 ms**.
- [ ] `getClientWriteTimeout = readTimeout + (detection + ringHops) * 5`, where `detection = 3*beat + M*N*2`, `ringHops = N*M*2`.
- [ ] `getElectionMaxDelay = ((3*beat + M*N*2) + N*M*2) * 5`. For `N=7`: about **17.8 s**; for `N=22`: about **23.8 s**.
- [ ] Happy-path write must use the minimal number of hops:
  - client → contacted replica: direct, no emulated delay;
  - contacted replica → coordinator: one `tell` hop;
  - coordinator → all UPDATE: one hop;
  - quorum replicas → coordinator ACK: one hop;
  - coordinator → all WRITEOK: one hop;
  - origin → client WriteResult: direct.
- [ ] Commit as soon as `Q = floor(N/2)+1` distinct ACKs are present. Never wait for all ACKs in the happy path.
- [ ] Keep exactly one update in flight. This avoids double-order bugs and still fits the timing budget.
- [ ] Do not add retries, sleeps, polling loops, or extra round trips to the happy path.
- [ ] Use `System.nanoTime()` for durations; do not compare wall-clock timestamps across actors.
- [ ] Heartbeat interval: 1000 ms. Coordinator-silence timeout: about `3 * beat`. Check timer every 200–250 ms.
- [ ] Election ACK timeout: a small multiple of `getMaxLatencyPlusTolerance()`, not seconds. A crashed next node must be skipped quickly.
- [ ] Sync timeout / election restart timeout: a few seconds, but comfortably below the client write timeout.
- [ ] TestsCommons says timings may change. Keep the protocol minimal and failure timeouts generous; do not hardcode assumptions that only hold for the current tests.

### C. Crash-injection semantics

- [ ] The base wrapper calls `crash(how)` and immediately logs/sends the `Crash` object to the probe. For deferred crash types, the **actual** crash happens later; do not confuse the two.
- [ ] `Crash.Type.Now`: immediate crash. Per the starter javadoc, `after_n_messages_of_type <= 0` also means immediate for any type.
- [ ] For deferred types with `N >= 1`, count messages processed/received **and** messages sent/broadcast. Hidden tests may attach `Heartbeat`, `Update`, `WriteOK`, or `Election` to either the coordinator (send side) or a replica (receive side).
- [ ] `Heartbeat`: coordinator crashes after sending N heartbeats; replica crashes after receiving N heartbeats.
- [ ] `Update`: coordinator crashes after broadcasting N UPDATEs; replica crashes after processing N UPDATEs.
- [ ] `WriteOK`: coordinator crashes after broadcasting N WRITEOKs; replica crashes after processing N WRITEOKs.
- [ ] `Election`: initiator/forwarder crashes after sending N ELECTIONs; receiver crashes after processing N ELECTIONs.
- [ ] Actual crash must: set `crashed = true`, cancel every timer, stop all sends, and ignore every incoming message including further `Crash` messages.
- [ ] A crashed replica must not send heartbeats, ACKs, ELECTIONs, SYNCs, or client responses. Messages already in a `NetworkChannel` may still be delivered; that is acceptable.
- [ ] Do not apply or callback after the crash flag is set. Check `crashed` at the top of every handler, timer, and send helper.
- [ ] Crash during election: surviving nodes must skip the crashed node via ACK timeout. If the initiator crashes, participants must time out and start a new election.
- [ ] Crash during synchronization: if the new coordinator dies, survivors must detect silence and re-elect; do not wait forever.
- [ ] Crash of the elected coordinator immediately after election: synchronization must not be the only way anyone learns the result; survivors need an election/sync timeout.
- [ ] `N=2` edge case: after a coordinator crash there is no quorum. Do not spin or crash the JVM; let client timeouts work and retry election slowly or not at all.
- [ ] The starter's `Crash` javadoc defines the threshold as messages **processed** (receive side). Count receive-side messages exactly as documented.
- [ ] Additionally count send-side protocol messages so `Crash.Update/WriteOK/Heartbeat/Election` can crash a coordinator/initiator during broadcast. This is an extension needed for the spec's crash points; document it.
- [ ] Do **not** count client requests, self ticks, or unrelated messages toward `Crash.Type` thresholds.
- [ ] For deferred crash types, the abstract wrapper's probe callback fires when the `Crash` config is received, not when the actual crash happens. Do not use that callback as proof that the replica has stopped; use logs/behavior or a dedicated debug hook.
- [ ] Write down your exact interpretation of “after N messages” in `docs/DECISIONS.md`: for `N >= 1`, process/send the message then crash; for `N <= 0`, crash immediately.
- [ ] `Crash.Now` and typed `N=0` are both immediate. To crash after the first event of a type, use `N=1`; to crash before the first event, use a dedicated test-only hook (not a reinterpretation of `N=0`).
- [ ] If hidden tests use `minLatency == maxLatency`, the starter `NetworkChannel` throws (`nextInt(0)`). Decide whether to patch that starter bug and keep the diff documented.

### D. Election and recovery invariants

- [ ] Quorum includes the coordinator: initialize `acks` with `self`.
- [ ] Count ACKs from **distinct** replica IDs; ignore duplicates and self-ACK messages.
- [ ] An UPDATE must be stored in history as `pending` **before** sending its ACK. If the coordinator dies before WRITEOK, that update must survive.
- [ ] Election messages must carry:
  - the union of observed histories, including pending updates;
  - each participant's latest observed `(epoch, seq)`;
  - enough information to choose the winner deterministically.
- [ ] Winner rule: newest observed update wins; tie-break by a deterministic rule, e.g. highest replica ID. Document the rule.
- [ ] Only choose among replicas that participated/were visited; never elect a known-crashed replica.
- [ ] Before serving new writes, the new coordinator must commit every pending update it learned about, in `(epoch, seq)` order, then synchronize all replicas.
- [ ] New epoch must be strictly greater than every epoch in the merged history and every known epoch; reset `seq = 0`.
- [ ] Apply updates in `(epoch, seq)` order on every replica. Deduplicate by `UpdateId`.
- [ ] Reject stale coordinator traffic: heartbeat/UPDATE/WRITEOK from an old epoch or old coordinator must not reset failure detection or overwrite state.
- [ ] Accept SYNCHRONIZATION only if it is for the current or a newer epoch; if two candidates exist at the same epoch, use a deterministic rule such as **higher `coordinatorId` wins**, and document it.
- [ ] Do not serve writes until synchronization reaches at least a quorum and all earlier messages to each destination are ordered before new ones (FIFO channels guarantee this).
- [ ] Prevent split elections: deterministic initiator/backoff, campaign ID, and epoch checks. The smallest known-alive replica starts immediately; larger IDs wait briefly and cancel if an ELECTION arrives.
- [ ] If the initiator dies mid-election, participants must eventually restart. If the winner dies before sync, same.
- [ ] If a replica misses synchronization but stays correct, it must catch up before applying later updates; FIFO from the coordinator to that replica guarantees sync-before-later-messages.
- [ ] Safety argument to put in the report: any update ACKed by a quorum intersects any election quorum, so at least one correct replica reports it; the new coordinator merges histories and commits it before new writes.
- [ ] Uniform agreement: once any replica applies an update, all correct replicas must eventually apply it. Synchronization plus reliable FIFO channels provides this.
- [ ] Total order: only the coordinator assigns IDs and commits one update at a time; WRITEOK dissemination is FIFO per destination.
- [ ] Exactly-once: same `UpdateId` must never produce two local applications or two callbacks.

### E. Starter-code gotchas

- [ ] `AbstractReplica.tell(Serializable, ActorRef)` is package-private and emulates latency/FIFO. Use it for every replica-to-replica message. Direct `dst.tell(...)` bypasses the delay and can fail timing tests.
- [ ] `NetworkChannel` creates one channel per `(sender, destination)` lazily; messages to the same destination preserve FIFO. Cross-sender ordering is not guaranteed.
- [ ] `InitSystem.group` is unmodifiable. Copy it if you need a local sorted list, but do not mutate it.
- [ ] `AbstractReplica.onCrashMsg` calls `crash(how)`, then logs, then sends the `Crash` object to the listener. Your `crash()` must be idempotent.
- [ ] `createBaseReceiveBuilder()` only matches `InitSystem` while `initialized == false`; do not expect a second init message to be handled.
- [ ] `AbstractClient` handles the test's `ReadRequest`/`WriteRequest` and calls your `sendRead`/`sendWrite`.
- [ ] `AbstractClient` and `AbstractReplica` provide `log`/`debug` that already prepend the actor prefix. Do not add `[Client ...]` or `[Replica ...]` yourself.
- [ ] `Logger` is globally disabled during tests. Never depend on logging for protocol behavior, and never call `Logger.enable()`/`setDestination*` from actor code.
- [ ] `Logger` writes a timestamp with milliseconds; all "official" logs must go through it.
- [ ] `NetworkChannel` uses `rnd.nextInt(maxLatency - minLatency)`; if hidden tests set `min == max`, this throws. Do not change the base unless necessary; note it as a known starter limitation.
- [ ] `getMaxLatencyPlusTolerance()` is available and useful for timeouts; it grows with N.
- [ ] `Main` currently calls `system.terminate()` immediately. A demo must keep the system alive long enough to observe the protocol.
- [ ] The repo has no Gradle wrapper. Generate it with Gradle 9.2.1 and commit it.
- [ ] Keep the public `props`/`propsWithListener` signatures unchanged; tests instantiate actors through them.
- [ ] Do not use `Thread.sleep`, `.join`, blocking futures, or shared mutable state inside actors.

### F. Report and submission hard constraints

- [ ] The PDF says the official project deadline is expected around **May 2027**, and presentation slots have their own submission deadlines. A 20-day plan is self-imposed: confirm your actual slot/submission deadline immediately and book before coding, not after.
- [ ] Use the official LaTeX template; English; 3–4 pages target, **6 pages maximum**. Over-limit reports are automatically rejected.
- [ ] Include: system model/assumptions; architecture/state; read path; write protocol; quorum/history; failure detection; ring election; synchronization/recovery; safety argument; implementation/test results; limitations.
- [ ] State all additional assumptions explicitly (timeouts, failure detector accuracy, FIFO, no recovery, majority correct, static membership, no Byzantine faults).
- [ ] Mandatory final paragraph: “Information about the use of LLM tools”, with each tool name and purpose. Keep the log current from Day 1.
- [ ] Book the slot and email **both** the instructor and the teaching assistants before the submission deadline.
- [ ] Check the shared FAQ before emailing specification questions; never ask TAs for the solution.
- [ ] Register the group with the Google Form even if you already have a slot.
- [ ] Submit `Surname1Surname2/Surname1Surname2.tgz` containing all source files + report PDF; verify by extracting and building from the archive.
- [ ] Do not include `build/`, `.gradle/`, IDE files, or `.git/` in the archive.
- [ ] Presentation: 12 minutes hard limit; rehearse with a timer; prepare 3–4 demos including at least one crash/recovery corner case; bring a working laptop and a backup archive.
- [ ] Confidentiality/plagiarism: do not share code or implementation details outside your teammate.

### G. What not to do

- [ ] Do not implement dynamic membership, persistence, Byzantine fault tolerance, leases, dashboards, authentication, or client retries beyond what is needed.
- [ ] Do not pipeline multiple updates unless the simple version is already green; one in flight is easier to prove and fast enough.
- [ ] Do not wait for all replicas before committing; quorum is enough.
- [ ] Do not rewrite the starter architecture or replace Akka with threads/sockets.
- [ ] Do not use global mutable state or pass mutable collections into messages without copying.
- [ ] Do not ignore a failing base test by changing the test; fix the implementation.
- [ ] Do not leave debugging output, sleep calls, or commented-out protocol code in the submission.

---

## 0.2 Exact callback and log contract

### Mandatory callbacks

Client:
- [ ] Read success: `callbackOnReadResult(new ReadResult(true, idx, value, contactedReplicaId))`
- [ ] Write success: `callbackOnWriteResult(new WriteResult(true, idx, value, contactedReplicaId))`
- [ ] Read timeout: `callbackOnReadTimeout(new ReadTimeout(getSelf(), contactedReplica, idx))`
- [ ] Write timeout: `callbackOnWriteTimeout(new WriteTimeout(getSelf(), contactedReplica, idx, value))`

Replica:
- [ ] After each committed/recovered update is written to `positions[]`: `callbackOnUpdateApplied(idx, value)`
- [ ] On first participation in an election: `callbackOnElectionStarted(crashedCoordinatorId)`
- [ ] When this replica becomes coordinator, and when it processes SYNCHRONIZATION: `callbackOnCoordinatorElected(newCoordinatorId)`

Rules:
- [ ] Callbacks are the **only** test-observable channel. Never invent listener messages.
- [ ] Call each exactly once per event; duplicates and omissions both fail tests.
- [ ] Call after the state change, not before.
- [ ] A timeout removes the request from the pending map; a late response must not produce a success callback.
- [ ] Success results use `true`; do not send a failure result for a request that is supposed to time out.
- [ ] `fromReplica` is always the replica the client contacted, even for a forwarded write.
- [ ] If you validate an invalid index, decide and document whether to reply `success = false`; never throw from the actor.
- [ ] Never call a callback after the replica is crashed.

### Required log lines

The wrapper adds `[Client <name>]` or `[Replica <id>]`; the timestamp is added by `Logger`.

- [ ] `requesting READ (<idx>) to <ReplicaID>`
- [ ] `requesting WRITE (<idx>, <val>) to <ReplicaID>`
- [ ] `READ complete (<isSuccess>, <idx>, <val>) from <ReplicaID>`
- [ ] `WRITE complete (<isSuccess>, <idx>, <val>) from <ReplicaID>`
- [ ] `TIMEOUT READ request to <ReplicaID> (<idx>)`
- [ ] `TIMEOUT WRITE request to <ReplicaID> (<idx>, <val>)`
- [ ] `applied update <epoch>:<sequence> (<idx>, <val>)`
- [ ] `CRASHED`

Notes:
- [ ] `callbackOnUpdateApplied` already logs `UPDATE APPLIED ...`; add the exact `applied update e:s (idx,val)` line yourself if you want the specification wording.
- [ ] `callbackOnReadResult`/`WriteResult` log a result object whose format is close but not identical to the example. Add the exact line before invoking the callback if strict log matching matters.
- [ ] `AbstractReplica.onCrashMsg` already logs `CRASHED: type (n)`; do not fight it. For deferred crashes, log the actual crash yourself without sending another probe message.
- [ ] Logs must not depend on `Logger` being enabled; tests disable it.
- [ ] Never use `System.out`/`System.err` for protocol logs.

---

## 0.3 Test-failure triage

| Symptom | Most likely cause | Fix |
|---|---|---|
| `oneClientWriteWaitRead` times out on `WriteResult` | extra hop/retry; not committing at quorum; client callback not invoked | count hops; commit at `Q`; invoke `callbackOnWriteResult` |
| `WriteResult.fromReplica` mismatch | coordinator replied instead of origin | origin replica must reply after applying WRITEOK |
| `ReadResult` wrong replica | coordinator served the read | read locally; use contacted replica id |
| `expectMsgClass(ReadTimeout)` gets nothing | client retried or replica replied late | remove pending on timeout; no retry on crashed target |
| `expectMsgEquals(ReadTimeout)` fails | constructor args wrong | use `getSelf()`, contacted replica, index/value |
| `callbackOnUpdateApplied` count wrong | duplicate apply via WRITEOK + sync or coordinator self-broadcast | dedupe by `UpdateId`; do not send WRITEOK to self |
| `callbackOnUpdateApplied` arrives after test's no-message window | heartbeat/other callback sent to probe, or delayed duplicate | only required callbacks; no duplicates |
| `callbackOnElectionStarted` count > 1 | called on every forward/duplicate | call once per campaign per replica |
| `callbackOnElectionStarted` missing for survivors | only initiator calls it | every replica calls on first join |
| `CoordinatorElected` IDs differ across probes | split election/two winners | deterministic initiator/backoff; campaign/epoch checks |
| `CoordinatorElected` missing on winner | winner doesn't call callback | call it when winner decides |
| `WithCrashes` write times out | election never starts/sync never completes/write not retried | heartbeat timeout, ring skip, sync ack, flush pending writes |
| `NoCrashes` monotonic read violation | updates applied out of order/multiple in flight | serialize updates; FIFO per destination |
| `replicasCrashNow` fails | `createBaseReceiveBuilder` not used or crash not immediate | use base builder; handle `Crash.Now` |
| Election takes too long | ACK timeout in seconds; ring retries too often | small ACK timeout; skip crashed quickly; total election timeout |
| False election in `NoCrashes` | heartbeat timeout too short or stale tick | 3×beat; cancel/generation-check timers |

---

## 0.4 Safety and liveness proof obligations

Put these in the report, even if briefly.

- [ ] **Quorum intersection:** any two strict-majority quorums intersect in at least one replica.
- [ ] **Durability of quorum-acked updates:** if an update received ACKs from a quorum, at least one correct replica has it in history; the new coordinator learns it through the election message.
- [ ] **Pending history:** storing an UPDATE before ACKing is what makes the durability argument valid.
- [ ] **Election merge:** the ELECTION message's history union contains every update known to any participant; the winner receives the merged history.
- [ ] **Recovery before new writes:** the winner commits every pending update in order before assigning new epoch IDs; therefore no update is reordered or lost.
- [ ] **Synchronization:** every correct replica eventually receives the full history over reliable FIFO channels and applies missing updates in order.
- [ ] **Uniform agreement:** if any replica applies `w`, then `w` was quorum-acked or included in a new coordinator's history; synchronization delivers it to all correct replicas.
- [ ] **Total order:** only one coordinator assigns `(epoch, seq)` and commits one update at a time; all replicas apply in `(epoch, seq)` order.
- [ ] **Integrity/exactly-once:** deduplication by `UpdateId` prevents double application.
- [ ] **Sequential consistency:** each replica applies the same total order; local reads observe a prefix of that order, so per-replica reads are monotonic.
- [ ] **Liveness assumptions:** at least a quorum of replicas is always correct; crash detection has no false positives; channels are reliable and FIFO; timeouts are long enough for normal messages.
- [ ] **If assumptions are violated:** no liveness guarantee; state this explicitly rather than claiming impossible guarantees.

### Read-your-writes nuance

- [ ] A local read immediately after a write to the **same replica** may return the old value if the write has not committed yet.
- [ ] The specification says reads are answered immediately; the provided tests wait after writes. Document this as an assumption.
- [ ] If you want to guarantee read-your-writes, queue same-client reads at the contacted replica behind that client's pending write. This adds complexity and may conflict with the "immediate" requirement; do not add it unless a test fails.

---

## 0.5 Demo recipes (prepare these early)

### Demo 1 — normal operation
- N=5, coordinator 0.
- Write `(idx=0, val=10)` through replica 2.
- Read index 0 from all replicas.
- Show: `WRITE complete (true, 0, 10) from 2`, five `UpdateApplied`, all reads return 10.

### Demo 2 — non-coordinator crashes
- N=7, coordinator 0; crash replicas 1 and 2 with `Crash.Now`.
- Write `(0, 20)` through replica 6; read from all survivors.
- Show: quorum still reached; one `UpdateApplied` per survivor; no election.

### Demo 3 — coordinator crash while idle
- N=7, coordinator 0; crash 0.
- Wait for heartbeat timeout.
- Show: `ElectionStarted` on survivors, same `CoordinatorElected` id everywhere, SYNCHRONIZATION.
- Write through replica 6; read from all survivors.

### Demo 4 — crash during UPDATE/WRITEOK
- N=5 or 7; coordinator 0; inject `Crash.Update`/`Crash.WriteOK` after 1 message.
- Show: pending update in history; election; winner commits it; all replicas converge; `UpdateApplied` exactly once.
- This is the strongest corner-case demo; rehearse it.
- `Crash.WriteOK` after 1 send/process simulates partial WRITEOK dissemination. For the exact “quorum reached, before first WRITEOK” window, add a small test-only hook in the commit path (e.g., a debug flag that calls `crashNow()` after quorum but before the first send) and document it; do not rely on `Crash.Type` alone for that precise instant.
- For report evidence, capture the pending update in the logs, the election, the winner's recovery, and the final equal states.

### Demo 5 — crash during election (optional)
- N=7; crash coordinator and one ring participant; show ACK timeout skip and election completion.

For every demo:
- [ ] Use Logger only; save output to a file.
- [ ] Pre-record a video/backup in case live execution fails.
- [ ] Keep each demo under 2–3 minutes.
- [ ] State what the audience should observe before running it.

---

## 0.6 Report page budget

Target 3.5–4 pages, hard maximum 6.

- [ ] 0.25 page: problem, system model, assumptions.
- [ ] 0.75 page: architecture, actor state, message set, why Akka.
- [ ] 1.0 page: read path, write protocol, quorum, update IDs, history.
- [ ] 1.0 page: failure detection, ring election, skipping, winner rule.
- [ ] 0.75 page: synchronization, interrupted-update recovery, safety proof.
- [ ] 0.25 page: implementation, tests, limitations, LLM disclosure.
- [ ] 1–2 diagrams: architecture/message flow and election ring.
- [ ] One table: timeout values and rationale.
- [ ] Do not paste large code blocks; explain design choices.
- [ ] Compile early; page limits are enforced automatically.

---

## 0.7 Pair workflow and LLM log

- [ ] Confirm the group has exactly two students; both must be able to explain every component.
- [ ] Day 1: both read the specification, starter code, and tests independently; compare notes.
- [ ] Suggested split: one owns write/quorum/read, one owns election/recovery; both write and review tests.
- [ ] Daily 15-minute sync: what is green, what is blocked, what changes design.
- [ ] Review each other's commits; no unreviewed protocol changes after Day 10.
- [ ] Keep `docs/DECISIONS.md` with every design choice and timeout.
- [ ] Keep an LLM log from Day 1:

| Date | Tool | Purpose | Files/sections affected |
|---|---|---|---|
| | | e.g. code drafting, test debugging, report editing | |

- [ ] Put the same information in the mandatory report section; if no LLM was used, say so.
- [ ] Do not share code or implementation details outside the pair.

---

## 0.8 Daily operating rules

- [ ] Start each session by running the smallest failing test, not the full suite.
- [ ] End each session with a green targeted test and a commit.
- [ ] Run the full suite at least once per day after Day 5.
- [ ] Timebox debugging to 45 minutes; then simplify, log, or ask a spec question.
- [ ] Never edit the provided tests to make them pass; create separate debug tests/Main.
- [ ] Keep test logs and a `TEST_MATRIX.md` with pass/fail history.
- [ ] Update the report outline while implementing, not only at the end.
- [ ] Back up the repo and demo logs daily.
- [ ] Freeze the protocol design by Day 1; record any change and re-run all tests.

---

## 0.9 Build and test commands

```bash
java -version                 # use JDK 17
gradle --version              # 9.2.1
gradle wrapper --gradle-version 9.2.1
./gradlew --version
./gradlew clean test
```

- [ ] Keep the starter dependency versions (Akka 2.6.13 / Scala 2.13, JUnit BOM 6.0.1). Do not upgrade Akka or replace the test framework as a “fix”; the tests and API are tied to this baseline.
- [ ] Use the Gradle wrapper in the repo; do not rely on a globally installed Gradle after setup.
- [ ] If the build fails, fix the JDK/Gradle environment before touching protocol code.

Targeted runs:

```bash
./gradlew test --tests 'it.unitn.ds.base.APICompliance'
./gradlew test --tests 'it.unitn.ds.base.NoCrashes'
./gradlew test --tests 'it.unitn.ds.base.WithCrashes'
./gradlew test --rerun-tasks
```

Repeat for flakiness:

```bash
for i in $(seq 1 10); do
  ./gradlew test --rerun-tasks || break
done
```

- [ ] Run tests from a fresh clone before submission.
- [ ] Verify the archive by extracting it elsewhere and running `./gradlew clean test`.
- [ ] Do not rely on Gradle's test cache when checking flakiness; use `--rerun-tasks`.
- [ ] If Gradle/JDK versions conflict, fix the environment before writing protocol code.

---

## 1. Recommended design to lock on Day 1

Do not redesign mid-project. This design is the fastest path through the provided tests.

### Actor state (replica)

- `id`, `group: Map<Integer, ActorRef>`, `N`, `initialized`, `crashed`
- `positions[POSITIONS_LIST_LENGTH]` (all zeros at init)
- `coordinatorId`, `epoch`, `nextSeq` (coordinator only), `synced`
- `history: Map<UpdateId, UpdateRecord>` (pending + applied)
- `pendingWrites: Map<requestId, PendingWrite>` (only the contacted replica stores these)
- `updateQueue` (coordinator: one update in flight at a time)
- `inFlightUpdate`, `acks: Set<Integer>`
- `lastCoordinatorContact`, timers/Cancellables
- election state: `campaign`, `inElection`, `crashedCoordId`, `visited`, `latestByReplica`, `mergedHistory`, `syncAcks`
- crash config + per-type counters

### Messages (all immutable, `Serializable`)

Client ↔ replica:
- `ClientReadReq(requestId, idx, client)`
- `ReadResp(requestId, idx, value, replicaId)`
- `ClientWriteReq(requestId, idx, val, client)`
- `WriteResp(requestId, idx, val, replicaId)` — success only after commit

Replica ↔ replica:
- `Update(updateId, idx, val, originId, originRef, clientRef, requestId)`
- `Ack(updateId, replicaId)`
- `WriteOk(updateId, idx, val, originId, originRef, clientRef, requestId)`
- `Heartbeat(epoch, coordinatorId)`
- `ForwardWrite(requestId, idx, val, clientRef, originId, originRef)`
- `Election(campaignId, crashedCoordId, initiatorId, history, latestByReplica, visited)`
- `ElectionAck(campaignId, fromId)`
- `ElectionResult(campaignId, winnerId, mergedHistory, newEpoch)`
- `Synchronization(campaignId, newCoordId, newEpoch, history)`
- `SyncAck(campaignId, replicaId)`

Self timers:
- `ReadTimeoutMsg`, `WriteTimeoutMsg`
- `HeartbeatTick`, `CoordinatorCheckTick`
- `ForwardTimeout`, `UpdateTimeout`, `ElectionAckTimeout`, `ElectionTimeout`, `SyncTimeout`

### Happy-path write

1. Client sends `ClientWriteReq` to contacted replica R.
2. R stores a `PendingWrite` and forwards `ForwardWrite` to coordinator C.
3. C enqueues the write; only one update in flight.
4. C assigns `(epoch, seq)`, stores it pending in history, broadcasts `Update` to all.
5. Each replica stores the pending update (if new), sends `Ack` to C.
6. C counts itself + ACKs; at `Q = floor(N/2)+1`, commits:
   - apply locally, log `applied update e:s (idx,val)`, invoke `callbackOnUpdateApplied`;
   - broadcast `WriteOk` to all others;
   - if origin is C, reply `WriteResp` to the client.
7. Origin R receives `WriteOk`, applies once, invokes callback once, replies `WriteResp` with `fromReplica = R.id`.
8. C starts the next queued write only after the current one commits.

### Read

- R replies immediately with `positions[idx]`; no coordinator involvement.
- Client cancels the read timer and invokes `callbackOnReadResult(true, idx, value, R.id)`.

### Client timers

- Use the timeout values provided by `getReadTimeoutDelay()` / `getWriteTimeoutDelay()`; do not shorten or extend them.
- Assign a unique `requestId`, store the pending request, and schedule a one-shot timeout.
- On a successful response: cancel the timer, remove the request, invoke the success callback.
- On timeout: remove the request, invoke the exact timeout callback; a later response is ignored.
- Do not retry a request to a crashed replica; the client timeout is the expected outcome.

### Failure detection

- On `InitSystem`: set `epoch = 0`, `seq = 0`, empty history, zero positions, `lastCoordinatorContact = now`; start timers with a non-zero initial delay.
- Initial coordinator broadcasts heartbeat every `getCoordinatorBeatInterval()` ms; a newly elected coordinator starts heartbeat only after synchronization reaches quorum.
- Replica records `lastCoordinatorContact` on heartbeat/UPDATE/WRITEOK/SYNCHRONIZATION from the **current** coordinator only.
- Ignore heartbeats from an old epoch/coordinator; do not let them reset the timeout.
- Check every ~200 ms; if silence > `3 * beatInterval`, suspect coordinator.
- A replica forwarding a write also starts a timeout; if no UPDATE/WRITEOK appears, suspect the coordinator.
- A replica that ACKed an UPDATE but sees no WRITEOK before timeout also suspects the coordinator.
- Timeout values must be generous enough to avoid false positives; tests assume accurate detection.

### Election

- Ring order = sorted replica IDs.
- To avoid split elections, only the smallest known-alive replica starts immediately; others back off by rank and cancel if an ELECTION arrives.
- `ELECTION` carries:
  - campaign ID, crashed coordinator ID, initiator ID;
  - union of update histories;
  - each participant's latest observed update ID;
  - visited set.
- On first participation:
  - invoke `callbackOnElectionStarted(crashedCoordId)` exactly once;
  - merge local history and latest update;
  - ACK the sender;
  - forward to next alive replica.
- Sender waits for ACK. On timeout, mark next as crashed, skip it, forward to the following one.
- Ring completion: when a forwarder's next alive replica is the initiator, mark the message `roundComplete = true` before sending it back. The initiator finalizes only when it receives `roundComplete`; a duplicate visit before that is ACKed and dropped.
- If the initiator crashes before completion, participants' election-wide timeout restarts the election with a new campaign.
- When the message returns to the initiator, choose winner:
  - replica whose latest observed update is newest;
  - tie-break by highest replica ID;
  - only among visited/alive replicas.
- Send `ElectionResult` with merged history to the winner.
- Winner:
  - sets `newEpoch = maxEpochInHistory + 1`;
  - applies all missing history entries locally (once each);
  - invokes `callbackOnCoordinatorElected(self)`;
  - broadcasts `Synchronization(newCoordId, newEpoch, fullHistory)` to all others;
  - counts itself plus `SyncAck`s from a quorum, then starts heartbeat and queued writes;
  - keeps sending synchronization to laggards; FIFO from the coordinator ensures they process it before later UPDATE/WRITEOK messages.
- Receiver of SYNCHRONIZATION:
  - applies missing updates in `(epoch, seq)` order exactly once;
  - sets coordinator/epoch, leaves election mode;
  - invokes `callbackOnCoordinatorElected(newCoordId)` once;
  - sends `SyncAck`.

### Interrupted-update recovery

- Store every observed UPDATE in history as `pending`, not only applied updates.
- Election merges all histories.
- Winner commits every pending update it learns about, in `(epoch, seq)` order, then synchronizes.
- Quorum intersection guarantees that any update ACKed by a quorum is known to at least one correct replica, so it survives the election.
- Deduplicate by `UpdateId`; `callbackOnUpdateApplied` must fire exactly once per replica per update.

### Writes during election

- Contacted replica queues the write while election/sync is in progress.
- After synchronization, if the request is already in history, reply success.
- Otherwise forward it to the new coordinator (or process it if this replica won).
- Keep request IDs to suppress duplicates.

### Protocol edge cases you must decide now

- [ ] `Q = floor(N/2) + 1`, always counting the coordinator itself.
- [ ] `N` odd/even: for `N=5`, `Q=3`; for `N=22`, `Q=12`; never hardcode a quorum.
- [ ] `N=2`: after the coordinator crashes, no quorum exists. Do not spin; let the client time out.
- [ ] `N=1`: `Q=1`; handle the trivial case without sending to yourself.
- [ ] Coordinator crashes before broadcasting UPDATE: the update was never observed, so there is nothing to recover; the origin may retry it after election or the client may time out.
- [ ] Coordinator crashes after some UPDATEs: pending history exists at recipients; our design commits observed pending updates during recovery, so an unacknowledged write may still commit; document this behavior.
- [ ] Coordinator crashes after quorum ACKs but before WRITEOK: the update must be recovered and committed.
- [ ] Coordinator crashes during WRITEOK: some replicas applied, some did not; synchronization must fill the gap.
- [ ] Coordinator crashes during heartbeat: replicas detect after timeout; no special case.
- [ ] Election initiator crashes: participants time out and restart election.
- [ ] A ring participant crashes after ACK but before forwarding: election-wide timeout detects the stall and restarts.
- [ ] Elected winner crashes before sync: replicas time out and re-elect.
- [ ] Winner crashes during sync: some replicas synced; others re-elect; merged history preserves updates.
- [ ] Two elections start concurrently: deterministic initiator/backoff; campaign IDs; only one winner per epoch; stale sync rejected.
- [ ] Old coordinator traffic arrives after new epoch: reject heartbeats/UPDATE/WRITEOK unless the update is in the synchronized history.
- [ ] Duplicate UPDATE/WRITEOK/SYNC: idempotent handling; exactly one apply/callback.
- [ ] Late client response after timeout: ignore; do not invoke a success callback.
- [ ] Invalid index: do not throw; decide between failure result and no response, and document it.
- [ ] Empty history election: winner is the highest-ID visited replica; no recovery updates.
- [ ] Write to a crashed replica: that replica ignores; client timeout; no other replica may reply on its behalf.
- [ ] Write forwarded to a crashed coordinator: origin times out, triggers election, re-forwards after sync.
- [ ] All replicas crash: test ends; no protocol obligations; no exceptions.
- [ ] Stale timer tick after re-election: generation token/cancel prevents false election.
- [ ] Mixed direct and emulated sends to the same destination: do not do it; ordering can break. Use `tell` for the whole protocol stream.

### Design rules that prevent most subtle bugs

- [ ] Use numeric replica IDs as map keys, not `ActorRef` equality.
- [ ] Keep `group` sorted for ring order; use `TreeMap`/sorted list.
- [ ] Make every message immutable and copy any collection before sending.
- [ ] Use `(epoch, seq)` as the primary update identity and ordering key.
- [ ] Keep a `pending` vs `applied` flag per update; never infer from the positions array.
- [ ] Apply updates in strict `(epoch, seq)` order; never apply a later update before an earlier known pending one.
- [ ] Reset `seq` only when starting a new epoch, after recovery updates are committed.
- [ ] Store `requestId` with pending writes and history records to dedupe client retries.
- [ ] Only the contacted/origin replica replies to its client.
- [ ] Coordinator certifies commit after quorum; it does not need a WRITEOK ACK for the happy path.
- [ ] Never send WRITEOK to yourself; apply locally once.
- [ ] Cancel timers on state transitions and use a generation token to ignore stale ticks.
- [ ] Check `crashed` before every send, timer, and state mutation.
- [ ] Do not process new writes until synchronization completes.
- [ ] Reject stale epoch/coordinator messages before touching state.
- [ ] Log decisions at key points, but never let logging affect control flow.

### Official assumptions to state in the report

- [ ] Static membership; `N` replicas known at initialization; no joins/rejoins.
- [ ] Crash-stop failures; crashed replicas do not recover.
- [ ] At all times, a strict majority `Q` of replicas is correct.
- [ ] Communication is reliable and FIFO; no message loss, duplication, or corruption.
- [ ] Network delays are emulated and bounded by the starter's min/max latency.
- [ ] Crash detection is accurate; no false positives (timeouts chosen reasonably).
- [ ] Clients may time out; a timed-out write may still commit later.
- [ ] No Byzantine faults, no network partitions, no malicious clients.
- [ ] No disk persistence; state is in memory.
- [ ] One coordinator per epoch; update IDs are unique within an epoch.
- [ ] Reads are local; sequential consistency is per replica, not global linearizability.
- [ ] A client's operations to the same replica preserve order via FIFO channels/mailboxes.
- [ ] Any additional timeout/assumption you introduce must be listed here and in the report.

---

## 2. 20-day plan

Assumes 5–7 focused hours/day. If you have less time, cut the P2 items first; never cut P0 tests, report, or submission.

| Day | Outcome | Main tasks |
|---|---|---|
| 1 | Build green + design frozen | Install JDK 17 and Gradle 9.2.1; run `./gradlew test`; read all starter classes/tests; write message/state table; download LaTeX template/slides; book presentation slot; start LLM log; create git repo; commit skeleton. |
| 2 | Read path works | Client `sendRead` + timer + callback; replica init, positions, local read reply; manual smoke test. |
| 3 | One write works | UPDATE/ACK/WRITEOK/quorum/history/apply; heartbeat; client `WriteResp`; run `APICompliance.oneClientWriteWaitRead`. |
| 4 | Total order + crash mode | One update in flight + queue; dedupe; `Crash.Now`, ignore all messages after crash, cancel timers; run `replicasCrashNow` and `crashReplicaAndTryRequests`. |
| 5 | `NoCrashes` green | Multiple writes, N=7/22, monotonic reads; fix all `NoCrashes`; run repeatedly. |
| 6 | Failure detection | Heartbeat tick/check; forward/update timeouts; suspect coordinator; queue writes. |
| 7 | Election happy path | Ring ELECTION/ACK, callbacks, winner, sync; run election APICompliance tests. |
| 8 | Election with crashes | ACK timeout, skip crashed nodes, quorum sync; run `WithCrashes.nonCoordinators...`. |
| 9 | Coordinator crash + client writes | Queue/retry writes, flush after sync; run `WithCrashes.coordinatorCrash...`. |
| 10 | Interrupted update recovery | Pending history in election, winner commits, sync applies; test crash during UPDATE/WRITEOK. |
| 11 | All crash injection points | Verify Now/Heartbeat/Update/WriteOK/Election, send and receive counters; custom tests. |
| 12 | Stress + flakiness | Full suite 10×; N=5/7/22; tune timeouts; no false elections; remove races. |
| 13 | Demo harness | Main/Demo scenarios: normal, non-coordinator crashes, coordinator crash, recovery; save logs. |
| 14 | Report draft | LaTeX template; architecture, protocol, election, safety, assumptions, diagrams. |
| 15 | Report complete | Test results, limitations, LLM section; check 3–4 pages / max 6. |
| 16 | Code quality | Refactor, Javadocs, formatting, remove debug, no direct prints; fresh clone build. |
| 17 | Presentation rehearsal | 12-minute run; 3–4 demos; Q&A list; timing. |
| 18 | Buffer / harden | Fix rehearsal and hidden-test edge cases; full suite again. |
| 19 | Package + submit | Verify page limit, all tests, archive name/structure, email, book slot. |
| 20 | Contingency | Final rehearsal, laptop/offline build, backup of archive. |

### Go/no-go checkpoints

- [ ] **End of Day 1:** build runs; design/messages/timeouts written; presentation slot booked; template downloaded.
- [ ] **End of Day 3:** `oneClientWriteWaitRead` green for N=7/22, coordinator 0/1.
- [ ] **End of Day 5:** all `NoCrashes` green repeatedly.
- [ ] **End of Day 9:** both `WithCrashes` tests green at least once.
- [ ] **End of Day 12:** full suite green 10×; no false elections; crash matrix covered.
- [ ] **End of Day 15:** report compiles within page limit; demos work.
- [ ] **End of Day 17:** 12-minute rehearsal complete; Q&A prepared.
- [ ] **End of Day 19:** archive submitted and verified.

If a checkpoint is missed:
- Day 3 missed → simplify write path; no elections yet.
- Day 5 missed → stop adding features; fix ordering/dedupe.
- Day 9 missed → focus only on coordinator-crash path and base tests; report draft in parallel.
- Day 12 missed → cut demo polish and optional scenarios; protect submission.
- Day 15 missed → finish report immediately; code is already worth more than extra features.

### Effort estimate

- 20 days × 5–7 focused hours ≈ 100–140 hours total for the pair.
- If you only have 3–4 hours/day, you need both teammates working consistently; do not leave election/recovery for the last week.
- The hardest 30% is election + recovery; budget at least 5 days there.
- Report/demo/submission need at least 3 days; do not compress them into the final 24 hours.
- Grading reality: a correct implementation of all requested functionality is worth 6 points; implementing a subset or relying on stronger assumptions can still pass with a lower mark. Protect correctness of the core and the base tests first.

### If you fall behind

Priority order:
1. All base tests green (especially `APICompliance` + `WithCrashes`).
2. Correct election/recovery under coordinator crash.
3. Report and submission.
4. Demo polish.
5. Extra features (none are required—do not add them).

Drop in this order: custom dashboards, extra CLI features, persistence, dynamic membership, optimization, fancy diagrams.

---

## 3. Detailed checklist

### Phase A — Setup (Day 1)

- [ ] Install JDK 17 (or verify Gradle 9.2.1 compatibility).
- [ ] Install/use Gradle 9.2.1; generate wrapper: `gradle wrapper --gradle-version 9.2.1`.
- [ ] Clone starter into your own repo; keep the original remote for reference.
- [ ] Run `./gradlew build` and `./gradlew test`; record baseline failures.
- [ ] Read `README.md`, `AbstractClient`, `AbstractReplica`, `Client`, `Replica`, `NetworkChannel`, `Logger`, all three test classes.
- [ ] Download the official LaTeX report template and project presentation slides.
- [ ] Confirm the two-student group and book the presentation slot; email instructor + both TAs.
- [ ] Create the LLM-usage log with columns date/tool/purpose/files.
- [ ] Write a one-page design note: messages, state, timeouts, election steps, recovery steps.
- [ ] Decide tie-break rule (highest ID) and timeout constants; document them.
- [ ] Create `docs/TEST_MATRIX.md` and `docs/DECISIONS.md`.
- [ ] Make first commit.

### Phase B — Client and read path (Day 2)

- [ ] `Client.sendRead`: assign request ID, store pending, start timeout, log request, send to replica.
- [ ] Client handles `ReadResp`: cancel timer, remove pending, log result, `callbackOnReadResult`.
- [ ] Client handles read timeout: remove pending, log exact timeout format, `callbackOnReadTimeout`.
- [ ] `Replica.initSystem`: store group, N, coordinator, initialize positions, start timers.
- [ ] `Replica.getSystemNumberOfActors()` returns group size.
- [ ] Replica handles `ClientReadReq`: immediate local reply; no coordinator.
- [ ] Smoke test read from all replicas; verify `fromReplica` is correct.
- [ ] Verify no direct `System.out` prints.

### Phase C — Happy-path write (Day 3)

- [ ] `Client.sendWrite`: request ID, pending map, timeout, log, send.
- [ ] Contacted replica stores pending write and forwards to coordinator.
- [ ] Coordinator queues writes; one in flight.
- [ ] Coordinator assigns `(epoch, seq)` and stores pending history.
- [ ] Coordinator broadcasts UPDATE via `tell`; counts self ACK.
- [ ] Replica handles UPDATE: store pending if new, ACK, crash counter.
- [ ] Coordinator collects distinct ACKs; at quorum commits.
- [ ] Commit: apply once, log required format, callback once.
- [ ] Broadcast WRITEOK; origin applies and replies; coordinator replies if origin.
- [ ] Client handles `WriteResp`: cancel timer, callback.
- [ ] Client handles write timeout: callback with exact fields.
- [ ] Run `APICompliance.oneClientWriteWaitRead` for coordinators 0/1, N=7/22.
- [ ] Run `callbackOnUpdateApplied...` tests.

### Phase D — Total order and crash mode (Day 4)

- [ ] Serialize all writes at coordinator; no concurrent updates.
- [ ] Deduplicate updates by `UpdateId`.
- [ ] Ensure per-replica apply order is `(epoch, seq)`.
- [ ] Implement `crash(Crash)`:
  - [ ] `Now` → immediate crashed mode.
  - [ ] Other types → count processed/sent messages, crash after N.
  - [ ] `N=0` → immediate.
- [ ] Crashed replica ignores all incoming messages.
- [ ] Crashed replica sends nothing; timers check flag/cancel.
- [ ] Log `CRASHED` (avoid breaking the abstract callback).
- [ ] Run `replicasCrashNow` and `crashReplicaAndTryRequests`.
- [ ] Verify read/write timeouts to a crashed replica use the expected constructors.

### Phase E — `NoCrashes` (Day 5)

- [ ] Support N=7 and N=22 without timing failures.
- [ ] Reads are local and monotonic per replica.
- [ ] Multiple rapid writes are queued and committed in order.
- [ ] `callbackOnUpdateApplied` once per write per replica.
- [ ] Run `NoCrashes.sequentialConsistencyOneWriteClient` 10×; fix flakiness.
- [ ] Confirm `getMaxUpdateDelay` budget: client→R (direct), R→C, C→all UPDATE, all→C ACK, C→all WRITEOK.

### Phase F — Failure detection (Day 6)

- [ ] Coordinator starts heartbeat after init; stops when no longer coordinator/crashed.
- [ ] Heartbeat every `getCoordinatorBeatInterval()`.
- [ ] Replica check timer (~200 ms) and 3×beat silence threshold.
- [ ] Reset last contact on heartbeat/UPDATE/WRITEOK/SYNC from current coordinator.
- [ ] Ignore stale heartbeats from old epoch/coordinator.
- [ ] Forward-write timeout detects dead coordinator.
- [ ] UPDATE-without-WRITEOK timeout detects dead coordinator.
- [ ] On suspicion: queue writes, start election after deterministic backoff.
- [ ] Verify no election in `NoCrashes` under load.
- [ ] Log suspicion for debugging (debug only).

### Phase G — Election happy path (Day 7)

- [ ] Define campaign ID and dedupe rule.
- [ ] Smallest alive replica initiates; others back off by rank.
- [ ] `callbackOnElectionStarted` exactly once per campaign per replica.
- [ ] Build ELECTION with local history/latest update.
- [ ] Receiver merges, ACKs sender, forwards to next alive.
- [ ] Initiator detects full ring and selects winner.
- [ ] Winner selection: newest `(epoch, seq)`, tie highest ID.
- [ ] Send `ElectionResult` with merged history.
- [ ] Winner calls `callbackOnCoordinatorElected(self)`.
- [ ] Winner applies pending history and broadcasts SYNCHRONIZATION.
- [ ] Replicas apply missing updates, callback, send SyncAck.
- [ ] Winner waits for quorum SyncAck, then resumes writes/heartbeat.
- [ ] Run `callbackOnElectionStarted...` and `callbackOnCoordinatorElected...` tests.

### Phase H — Election with crashed nodes (Day 8)

- [ ] ELECTION ACK timeout; mark next crashed; skip to following.
- [ ] Never wait forever for a crashed node.
- [ ] Ring wraps correctly; no duplicate visits.
- [ ] Quorum sync works with exactly a majority alive.
- [ ] Crashed replicas ignore ELECTION/SYNC.
- [ ] Run `WithCrashes.nonCoordinatorsCrashClientWritesWaitsReads` for N=7/22.
- [ ] Tune ACK timeout to a small multiple of max latency, not seconds.

### Phase I — Coordinator crash with pending client writes (Day 9)

- [ ] Origin queues writes while election/sync is in progress.
- [ ] After sync, check history for request ID:
  - [ ] found/applied → reply success;
  - [ ] not found → forward to new coordinator.
- [ ] Origin retries only after confirmed coordinator failure.
- [ ] Avoid duplicate `UpdateApplied` callbacks.
- [ ] Run `WithCrashes.coordinatorCrashClientWritesWaitsReads` for 1/7 and 0/22.
- [ ] Verify `fromReplica` is the contacted replica, not the coordinator.

### Phase J — Recovery of interrupted updates (Day 10)

- [ ] Store pending UPDATEs in history before ACK.
- [ ] ELECTION carries all observed history, including pending.
- [ ] Winner commits pending updates in order before new epoch writes.
- [ ] Synchronization includes full history; receivers apply missing only.
- [ ] Deduplicate applied updates by `UpdateId`.
- [ ] Test: crash coordinator after quorum ACK but before WRITEOK.
- [ ] Test: crash coordinator during WRITEOK dissemination.
- [ ] Test: crash a replica during UPDATE; quorum still commits.
- [ ] Confirm final state equal on all correct replicas.

### Phase K — Crash injection matrix (Day 11)

- [ ] `Crash.Now`.
- [ ] `Crash.Heartbeat` on coordinator (after N sends) and replica (after N receives).
- [ ] `Crash.Update` on coordinator (after N broadcasts) and replica (after N receives).
- [ ] `Crash.WriteOK` on coordinator (after N broadcasts) and replica (after N receives).
- [ ] `Crash.Election` on initiator/forwarder/receiver.
- [ ] Crash during sync and during election wait.
- [ ] Crash new coordinator immediately after election.
- [ ] Assert no exceptions, no duplicate callbacks, no client callback after timeout.
- [ ] Add these as repeatable scenarios in `docs/TEST_MATRIX.md`.

### Phase L — Stress and flakiness (Day 12)

- [ ] Run full `./gradlew test` at least 10 times.
- [ ] Run with N=5, 7, 10, 22; coordinators 0 and 1.
- [ ] Run with min/max latency variations if hidden tests do.
- [ ] Check no false-positive elections.
- [ ] Check no missed client timeouts.
- [ ] Check no message sends after crash.
- [ ] Check logs have timestamps and required formats.
- [ ] Remove all `Thread.sleep` from actor code.
- [ ] Profile test duration; N=22 tests are bounded by windows, not your protocol.

### Phase M — Demo harness (Day 13)

- [ ] Fix `Main` so it does not terminate immediately.
- [ ] Implement the required scenario runner: a sequence of write operations, possibly interleaved with crash events, while a client repeatedly performs reads.
- [ ] Keep the runner deterministic enough to reproduce and present; expose it via command-line modes or a small scenario list.
- [ ] Create scripted scenarios:
  - [ ] normal write/read;
  - [ ] non-coordinator crashes;
  - [ ] coordinator crashes before write;
  - [ ] coordinator crashes during UPDATE/WRITEOK;
  - [ ] crash during election.
- [ ] Print protocol logs to file/stdout via Logger only.
- [ ] Show final positions on all replicas.
- [ ] Save outputs/logs for report and backup demo.
- [ ] Keep scenarios deterministic enough to present in 12 minutes.

### Phase N — Report (Days 14–15)

- [ ] Download the official LaTeX template and presentation slides.
- [ ] Read the presentation slides' example questions and make sure each is answered somewhere in the report.
- [ ] Write section: system model and assumptions.
- [ ] Write section: architecture and actor state.
- [ ] Write section: read path and two-phase write protocol.
- [ ] Write section: quorum, history, total order, sequential consistency.
- [ ] Write section: heartbeat/failure detection and timeouts.
- [ ] Write section: ring election, skipping, winner rule.
- [ ] Write section: synchronization and interrupted-update recovery.
- [ ] Write safety argument: quorum intersection + history + sync.
- [ ] Write implementation details: Akka, network delays, crash injection, logging.
- [ ] Include test results and 1–2 diagrams.
- [ ] State all additional assumptions.
- [ ] Add mandatory “Information about the use of LLM tools” section.
- [ ] Compile PDF; check 3–4 pages (max 6).
- [ ] Proofread English and references.

### Phase O — Code quality (Day 16)

- [ ] Remove unused imports, TODOs, dead code, debug prints.
- [ ] Consistent formatting and naming.
- [ ] Javadocs for message classes and major methods.
- [ ] No shared mutable state; messages immutable.
- [ ] Constants for timeouts and quorum.
- [ ] Fresh clone + `./gradlew clean test` passes.
- [ ] Verify no build artifacts in submission.

### Phase P — Presentation (Day 17)

- [ ] Build 12-minute slide deck:
  - [ ] 1 min problem/requirements;
  - [ ] 2 min architecture;
  - [ ] 3 min write protocol;
  - [ ] 3 min election/recovery;
  - [ ] 2 min demos/results;
  - [ ] 1 min lessons/assumptions.
- [ ] Rehearse with timer.
- [ ] Prepare 3–4 demos, including corner cases.
- [ ] Prepare Q&A answers (see Section 6), including showing code, executing a scenario, and describing extensions.
- [ ] Test on the laptop you will use; offline build; backup archive.
- [ ] Confirm whether the presentation is in person or online and test the setup.

### Phase Q — Submission (Days 19–20)

- [ ] Register group via Google Form.
- [ ] Book presentation slot; email instructor + both TAs before deadline.
- [ ] Verify all tests pass from a clean clone.
- [ ] Report PDF page count ≤ 6 (target 3–4).
- [ ] Directory named `Surname1Surname2` containing source + report.
- [ ] `tar -czvf Surname1Surname2.tgz Surname1Surname2`.
- [ ] Extract archive elsewhere and build/test again.
- [ ] Email submission with required details.
- [ ] Keep a backup copy and demo logs.

---

## 4. Test matrix to keep green

| Scenario | N | Coordinator | Expected |
|---|---|---|---|
| Basic read | 5,7,22 | 0,1 | immediate local value, correct `fromReplica` |
| One write then read | 7,22 | 0,1 | write success, all replicas eventually value |
| 5 rapid writes + concurrent reads | 7,22 | 0,1 | monotonic per replica |
| One write, exactly-once callbacks | 5 | 0,1 | one UpdateApplied per replica |
| Crash all replicas Now | 10 | 0 | Crash callback, no exceptions |
| Client requests crashed replica | 2 | 0,1 | ReadTimeout/WriteTimeout with exact args |
| Crash coordinator | 5,7 | 0,1 | ElectionStarted, all agree on new coordinator |
| Crash coordinator + 2 replicas | 7,22 | 1,0 | write eventually succeeds, fromReplica = target |
| Crash 9 non-coordinators | 22 | 0 | write succeeds with quorum |
| Crash during UPDATE | 5,7 | 0 | recovered, final state consistent |
| Crash after quorum before WRITEOK | 5,7 | 0 | pending update recovered |
| Crash during WRITEOK | 5,7 | 0 | missed replicas catch up |
| Crash during election | 5,7 | 0 | ring skips, election completes |
| Election initiator crashes mid-ring | 5,7 | 0 | timeout/restart, new election completes |
| Elected coordinator crashes during sync | 5,7 | 0 | new election, consistent state |
| Two sequential coordinator crashes | 7 | 0 | second election works, epoch increases |
| Crash Heartbeat/Update/WriteOK/Election injection | 5,7 | 0 | crash at configured point; recovery still safe |
| Election callbacks exactly once | 5,7 | 0,1 | one ElectionStarted and one CoordinatorElected per participant |
| Winner calls CoordinatorElected on itself | 5,7 | 0,1 | event with `replicaId == newCoordinatorId` |
| Write from same client, same replica, multiple values | 5 | 0 | WriteResults in order; final value correct |
| Read after write to same replica | 5 | 0 | document whether it may see old value |
| Invalid index | 5 | 0 | no actor crash; defined failure behavior |
| N=2 coordinator crash | 2 | 0 | no quorum; client timeouts; no infinite loop |
| N=1 trivial write/read | 1 | 0 | works without self-sends |
| Old coordinator message after sync | 5,7 | 0 | rejected, state unchanged |

Expected callback ordering for a normal election:
1. `ElectionStarted` on each participant when it first joins.
2. `CoordinatorElected` on the winner when it decides.
3. `CoordinatorElected` on every other participant when it processes SYNCHRONIZATION.
4. Any recovered `UpdateApplied` callbacks when the winner/replicas apply missing history; do not duplicate.

For every scenario record: pass/fail, runtime, final values per replica, callbacks observed, logs location.

---

## 5. Exact first 48 hours

### First 4 hours
1. Install JDK 17 + Gradle 9.2.1.
2. Clone starter, run baseline tests.
3. Read abstract classes and tests; write the message/state table.
4. Download the LaTeX template/slides; confirm the two-student group; book the presentation slot; start the LLM log.
5. Create git repo and commit.

### Next 4 hours
5. Implement Client timers + callbacks.
6. Implement Replica init + local read.
7. Smoke test read from every replica.

### Next 8 hours
8. Implement UPDATE/ACK/WRITEOK/quorum.
9. Implement WriteResult path.
10. Run `APICompliance.oneClientWriteWaitRead` until green.

### Next 8 hours
11. Add write serialization/dedupe.
12. Implement crash mode and ignore-after-crash.
13. Run `APICompliance` write/crash tests.
14. Commit and write down any deviations.

---

## 6. Q&A preparation (report + presentation)

Be ready to answer:
- Why reads are local; what consistency does that provide?
- Why a strict majority quorum? What does quorum intersection guarantee?
- Why keep pending updates in history, not only applied ones?
- What happens if the coordinator crashes before WRITEOK? After WRITEOK?
- What happens if the elected coordinator crashes during synchronization?
- How do you prevent two coordinators/split brain?
- Why tie-break by highest ID? Is the rule deterministic for all replicas?
- Why is sequence reset per epoch? How do you order across epochs?
- How do you ensure exactly-once `UpdateApplied` callbacks?
- How do you avoid duplicate writes when retrying after election?
- What are the timeout values and why are they safe?
- What assumptions are you making (FIFO, no recovery, majority correct, accurate detection)?
- Why allow only one update in flight? What does it simplify, and what is the throughput cost?
- Why commit at quorum instead of waiting for all replicas?
- What happens if two elections occur at the same epoch? How do you break the tie?
- What happens if the winner crashes halfway through synchronization?
- A client times out but the update later commits: is that allowed? What does the client observe?
- Why carry the full history in ELECTION/SYNCHRONIZATION? What is the size trade-off?
- How do you handle N=2 after a coordinator crash, where no quorum exists?
- How do you reject messages from an old coordinator after a new epoch?
- How do you test crash injection at each protocol point?
- What are the limitations (history size, one update in flight, no persistence)?
- How would you extend to dynamic membership or disk persistence?

---

## 7. Risk register

| Risk | Mitigation |
|---|---|
| Split election / two coordinators | deterministic initiator + backoff; campaign IDs; epoch checks; only current coordinator sends UPDATE/WRITEOK |
| False crash detection in tests | heartbeat 1000 ms; timeout 3×beat; reset on all coordinator traffic |
| Test timing failure on N=22 | one update in flight but no extra hops; direct client sends; commit at quorum, not all |
| Duplicate `UpdateApplied` | dedupe by `UpdateId`; apply once per replica |
| Lost pending update after coordinator crash | store pending UPDATE before ACK; election merges histories; winner commits before new writes |
| Client write stuck during election | queue at origin; check history after sync; retry only if not committed |
| Crashed node still sends | check `crashed` in every handler/send/timer; cancel timers |
| Calls after timeout | pending request map; ignore late responses; cancel timer on response |
| Direct stdout breaks tests | only `Logger`; no `System.out` in actors |
| Report over page limit | write to 3–4 pages; compile early; trim |
| Solo submission invalid | confirm exactly two students; contact instructor if not |
| Slot/submission deadline missed | book in the first 48 hours; confirm deadlines; email all recipients |
| Crash semantics misinterpreted | document receive/send counters and N=0 rule; test each type |
| Read-after-write from same replica sees old value | document assumption; optionally queue same-client reads behind pending write |
| Duplicate heartbeat/election timers after re-election | cancel old timers and use a generation token |
| Sync reaches quorum but some replicas lag | send sync to all; FIFO guarantees laggards process it before later updates |
| Two elections produce different winners | deterministic initiator/backoff, campaign IDs, epoch/coordinator tie-break |
| Presentation/demo fails live | pre-record backup video; test laptop/offline build |

---

## 8. Definition of done

- [ ] `./gradlew clean test` passes repeatedly from a fresh clone, with the provided tests unmodified.
- [ ] All required logs appear with timestamps.
- [ ] Demo shows normal operation and at least three failure/recovery cases, including the required write/crash/read sequence runner.
- [ ] Report is 3–4 pages (max 6), English, LaTeX, and ends with the mandatory LLM-tools section.
- [ ] Code is formatted, no direct prints, no debug leftovers, no shared mutable state.
- [ ] Exactly two students confirmed; presentation slot booked; instructor and TAs emailed.
- [ ] Archive structure and name are correct; extracted archive builds and passes tests; submission email sent.
- [ ] Presentation rehearsed under 12 minutes with 3–4 demos, backup video, and working laptop.
