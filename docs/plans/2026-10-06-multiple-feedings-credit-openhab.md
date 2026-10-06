# Wallet-Funded Multiple Feedings and OpenHAB Implementation Plan

> **For agentic workers:** Implement task by task using `superpowers:executing-plans` when available. This is a coding-agent handoff, not authorization to implement or activate physical feeding during the planning session. Record source pins and tests. This revision replaces the previous separate-credit-ledger design in full.

**Goal:** Use the actual LNbits herd-wallet balance as available feeding funds. Complete one feeding, transfer exactly its trigger amount into the CyberHerd/SplitPayments source wallet, and repeat while the remaining wallet balance funds another feeding.

**Operator decision, 2026-10-06:** No separate feeding-credit balance is needed. With a 1,000-sat threshold, a 2,340-sat wallet funds two feedings and two 1,000-sat splits transfers, leaving 340 sats in that wallet. Preserve the exact-transfer requirement from the previous clarification.

**Architecture:** LNbits remains the only monetary ledger. A background worker maintains durable feeding-attempt and transfer-work records solely to prevent repeated physical operations after failures. Each wallet has at most one unfinished feed/transfer cycle. The existing trusted home gateway and sole OpenHAB owner provide correlated completion and durable replay protection.

**Tech stack:** Python/asyncio, LNbits extension APIs (minimum declared version 1.3.0), SQLite/PostgreSQL, existing FastAPI/dashboard, CyberHerd/SplitPayments, existing Rust home gateway and OpenHAB JavaScript owner. Required changes span Lightning Goats extension, CyberHerd extension and the HOME gateway/owner source.

**Spec:** Sections 2–5 define the implementation contract. This is proposed work, not a claim that the system has already been changed or deployed. The historical filename is retained so existing handoff links keep working.

## 1. Inspected source and cause of the current behavior

Source pins inspected on 2026-10-06 (refresh before implementation):

- [Lightning Goats extension `499a2acca0b28c83d4720317f39e1bc14c8bb686`](https://github.com/lightning-goats/lightning_goats_extension/tree/499a2acca0b28c83d4720317f39e1bc14c8bb686), version 0.2.1.
- [CyberHerd extension `6437565cfea27da1685d80bab14e4cf9fb650dba`](https://github.com/lightning-goats/cyberherd_extension/tree/6437565cfea27da1685d80bab14e4cf9fb650dba), version 0.2.6.
- [Reference daemon/gateway `10c3dcb41432f672e0d947728db90e6d8bb2152b`](https://github.com/lightning-goats/lightning-goats/tree/10c3dcb41432f672e0d947728db90e6d8bb2152b).

| Existing location | Observed behavior | Required change |
| --- | --- | --- |
| Extension `tasks.py::_handle_payment` | Checks wallet balance, fires once and distributes/delegates | Wake a persistent one-cycle-at-a-time worker |
| Extension `_last_feed_monotonic` | Process-local 60-second cooldown; no independent backlog drain | Durable cooldown and periodic balance recheck |
| CyberHerd `services/send_splits.py::transfer_herd_balance_to_source` | No amount parameter; internal invoice transfers the whole balance | Add exact-amount, durable idempotent transfer API |
| CyberHerd `maybe_send_splits_for_user` and coordinator `_check_and_trigger_splits` | Can sweep the whole wallet when an incoming payment meets threshold | Suppress automatic whole-balance sweeps for coordinated accounts |
| CyberHerd `services/splits.py` | Configures SplitPayments targets on `source_wallet` | Retain target/share management and onward payout path |
| Extension `services/openhab.py::trigger_feeder` | HTTP 200 from `/runnow` means success | Correlated gateway completion before creating transfer obligation |
| Extension `get_feeder_override_state` | Non-ON text other than `None` can count as OFF | Only explicit ON/OFF is valid; unknown fails closed |
| Dashboard/API | Wallet balance drives progress | Retain this model; add backlog, active cycle and transfer status |

Read the pinned gateway's `src/gateway/{client,server,store}.rs`, `src/openhab.rs`, `docs/security/openhab-feeder-gateway.md`, `docs/security/openhab-owner-contract.md`, `docs/deployment/home-gateway-agent-handoff.md`, and `deploy/openhab/feeder-owner-v2.js`. Its credit-ledger implementation is a behavioral reference only: do not copy that separate monetary ledger into LNbits.

The reference owner uses `GoatFeeder_ManualRequest`, `GoatFeeder_ManualResult`, `GoatFeeder_OwnerLedgerV2`, `Goat_Plugs_Outlet2_Switch`, and `GoatFeedings`. It denies new work at 32 retained records / 8192 bytes. Verify deployed source, item names, one-second pulse and independent OFF/expire metadata; repository observations are not a live audit.

## 2. Required behavior and global constraints

### Normal cycle

1. With no unfinished cycle, read the current authoritative LNbits spendable herd-wallet balance. Validate wallet ownership, configured source wallet, coordinated CyberHerd capability, queue enablement and physical safety.
2. If the balance is at least the positive trigger amount and cooldown/cap permits, atomically create one durable feeding attempt with UUID, user, canonical herd/source wallet IDs and snapshotted threshold. This is an operational reservation, not a wallet debit or duplicate credit balance.
3. Submit that UUID through the gateway. No generic ACK, timeout or counter change by itself establishes completion.
4. On correlated completion, atomically record `feed_confirmed` and create the unique transfer obligation `feed:<UUID>:splits` for the stored threshold.
5. Transfer that exact amount using the normal LNbits internal invoice/payment path through CyberHerd. Wait until the specific transfer is authoritatively settled and its debit is reflected by the core wallet accounting path. Only then complete/release the cycle.
6. After the configured cooldown, read the current wallet balance again and repeat if it still meets the threshold. No further incoming payment is necessary. Do not precompute a fixed number of pulses from an old balance.

| Step, threshold 1,000 sats | Herd-wallet balance | Splits transfer |
| --- | ---: | ---: |
| Starting balance | 2,340 | — |
| First feeding confirmed, transfer still pending | 2,340* | Pending 1,000 |
| First transfer settled | 1,340 | 1,000 |
| Second feeding and transfer completed | 340 | Another 1,000 |
| Later 660 sats received | 1,000 | — |
| Third feeding and transfer completed | 0 | Another 1,000 |

*LNbits may already reserve/debit funds during a pending internal payment. Use the core's actual spendable-balance semantics; the important invariant is that the active cycle blocks a second physical feeding until the transfer settles. The table assumes no unrelated withdrawals, deposits or unexpected fees during each illustrated cycle.

### Constraints

- **No secondary monetary ledger:** do not implement `feed_credit_entries`, stored `credit_msat`, credit grants, opening-credit imports or credit-start boundaries. LNbits records every deposit/withdrawal. Keeping attempt amounts and transfer obligations is required operational history, not another spendable balance.
- Existing wallet funds are eligible once the operator explicitly enables this feature after reviewing the balance/backlog. No invoice replay or historical-credit reconstruction is needed. Deposits during downtime are naturally reflected on restart.
- Read integer millisatoshis; thresholds are integer sats. Preserve fractional-sat remainder in LNbits. Do not calculate money using floats or directly mutate core balances.
- Preserve the **60-second default** `LIGHTNING_GOATS_FEEDER_COOLDOWN_SECONDS`. Persist `next_attempt_at`; physical-owner minimum interval/cap remains authoritative even with a zero extension interval. Completion anchors the feed cooldown, and transfer completion is an additional prerequisite.
- One unfinished feed/transfer cycle per wallet, enforced in the database. Incoming payment callbacks only notify/wake the worker and handle idempotent presentation; they cannot bypass its guard. Duplicate callbacks therefore do not manufacture funds or feedings.
- Pending/ambiguous physical delivery: preserve the same UUID, poll status and block replacement attempts. A 404 or failed HTTP request is not proof of non-dispatch. After a terminal no-dispatch tombstone, honor the persistent retry delay before a new cycle.
- Confirmed physical feeding with pending/ambiguous/failed splits transfer: **retry or reconcile only the transfer; never repeat the feeding**. Keep the wallet cycle blocked even when its displayed balance remains above threshold.
- No physical/financial atomicity is claimed. An unrelated withdrawal can race a balance check. Use a dedicated herd wallet, disable competing sweep callers and show an insufficient-funds obligation if funds disappear after feeding. Do not invent an unsupported LNbits wallet hold. If the pinned core provides a suitable supported reservation, characterize it before optional use.
- An empty wallet is not success for an owed transfer. Insufficient funds pauses new physical feedings until the same obligation can be paid or explicitly reconciled against real payment evidence.
- Threshold/source setting changes affect future cycles only; pending cycles retain their amount and wallet IDs. Reject wallet reassignment while a cycle is unresolved. Do not convert an unresolved feeding into another wallet's obligation.
- Pause/override prevents new physical dispatch; receipt accounting stays in LNbits. Completion lookup and settlement of an already confirmed feeding's transfer can continue while paused. Disabling the entire extension stops its tasks; unfinished records recover on restart.
- All physical callers share one owner. Manual feeding creates no automatic splits obligation unless separately authorized; use the same safety/cooldown path and no legacy bypass.
- Do not operate both the Rust daemon and this extension against the same funded wallet/receipt flow. A shared owner does not eliminate duplicated economic intent from two controllers.
- Keep LNbits >=1.3.0 compatibility unless a reviewed requirement changes it. Verify transaction behavior on supported versions: a connection context is not necessarily a transaction, and some LNbits `Connection.execute` methods commit each statement.
- Preserve loopback URL support; keep gateway credentials separate from broad OpenHAB credentials. No secrets or private payment/request identifiers in public messages.
- No fiat/XMR work, new payment backend, feed-quantity changes or general CyberHerd rewrite.

### Status and progress

Keep `/api/v1/balance`, `balance_sats` and progress based on the actual wallet balance. Add `balance_msat`, `funded_feedings = balance_msat // (threshold_sats * 1000)`, `remainder_msat`, `remainder_sats`, `active_attempt_id`, `active_threshold_sats`, `queue_state`, `next_attempt_at`, `distribution_status` and `distribution_owed_sats`.

`funded_feedings` is a raw balance-based estimate, not permission to dispatch: the balance may still include funds owed for the current completed feeding. Label that condition and show the active transfer separately. Do not subtract an owed amount from a core balance that has already reserved/debited it. Avoid a second persisted “available credits” total.

Queue states: `idle`, `ready`, `cooldown`, `override`, `disabled`, `safety_unavailable`, `capacity`, `pending`, `ambiguous`, `distribution_pending`, `distribution_ambiguous`, `distribution_insufficient_funds`. Progress uses bounded integer arithmetic and stays full while the actual balance meets threshold.

## 3. Operational persistence and extension interfaces

Add migration `m006_feed_cycles`, preserving migrations 001–005 and existing payment audit rows. Use prefixed tables and explicit portable transactions verified against SQLite and PostgreSQL:

| Table | Purpose and fields |
| --- | --- |
| `feed_wallet_state` | Canonical `wallet_id` PK, owner, `enabled` default false on upgrade, `active_attempt_id`, `next_attempt_at`; synchronization/control only, no monetary balance |
| `feed_attempts` | UUID, user, canonical herd/source wallet IDs, immutable threshold, phase, physical outcome, transfer key/reference, timestamps and bounded error/reconciliation reason |
| `feed_outbox` | Unique event/obligation key, attempt reference, pinned transfer amount/destination when applicable, status/retry time; separate messaging from financial work |

Account for durable transitions: `intent → pending/ambiguous → feed_confirmed → transfer_pending/transfer_ambiguous → complete`. A correlated terminal `not_dispatched` releases the cycle after recording cooldown; an operator's not-fed reconciliation requires the HOME request to be authoritatively non-executable first. An operator's fed reconciliation must create the same unique transfer obligation, not a second feeding.

On confirmation, record physical completion and the transfer obligation in one local transaction. On transfer settlement, record the authoritative LNbits payment identity and release the wallet guard in one local transaction. Crash before either commit is reconciled against the same remote request/payment, never by creating a replacement. Never hold a database transaction during a network call.

Enforce the wallet's active-cycle guard with a conditional update/row lock, not only an asyncio lock. Only the transaction winner that creates the intent may issue its initial POST; a worker finding an existing intent performs status reconciliation only. Before any POST, re-read wallet funds and safety after acquiring the guard. If funds became insufficient before dispatch, durably cancel that never-dispatched intent and release it. After any possibly sent POST, uncertainty must remain blocked.

Create `feed_models.py`, `feed_crud.py`, `services/feeding.py` with these contracts:

- `get_feed_snapshot(wallet_id: str, threshold_sats: int) -> FeedSnapshot`: fresh core balance plus cycle/queue state; no balance write.
- `begin_feed_attempt(wallet_id: str, *, source_wallet_id: str, threshold_sats: int, now: int) -> FeedAttempt | None`: persistent exclusive cycle guard, snapshotted identity/amount.
- `apply_feed_outcome(attempt_id: UUID, outcome: GatewayOutcome, now: int) -> FeedAttempt`: matching correlated physical outcome and unique outbox work.
- `apply_transfer_outcome(attempt_id: UUID, outcome: TransferOutcome, now: int) -> FeedAttempt`: only matching authoritative settled payment completes the cycle.
- `reconcile_feed_attempt(attempt_id: UUID, *, outcome: Literal['fed','not_fed'], actor_user_id: str, reason: str) -> FeedAttempt`: authenticated owner and evidence; synchronize HOME state before allowing replacement work.
- `run_feed_step(settings) -> FeedStepResult`: resolve an existing cycle first; otherwise attempt one eligible cycle.
- `run_feed_worker() -> None`: unique permanent task checking enabled accounts every two seconds, serial per wallet; bounded independent account work.

Keep `processed_payments` idempotency/proof validation and historical-notification filtering for payment messages. Do not use those rows to compute money, and do not require another receipt or successful message to continue a funded wallet. The stale-payment reconciler must not reset feed/transfer attempts.

## 4. Gateway and OpenHAB changes

### 4.1 Reuse the existing gateway wire contract

Add `services/feeder_gateway.py`. Keep `services/openhab.py` for existing read-only price/safety integration until separately migrated. Automatic and manual physical requests use the gateway exclusively.

Configuration: `feeder_gateway_url`, protected `feeder_gateway_token`, and persisted `automatic_feeding_enabled=false` on upgrade. Verify the actual gateway authentication scheme from its source; do not invent a different header convention. Use existing outbound URL validation and disable redirects for authenticated requests.

Reuse:

- `GET /v1/feeder/override` for trusted override/remote-enable state.
- `POST /v1/feeder/request/<UUID>` to submit a committed attempt.
- `GET /v1/feeder/request/<UUID>` to reconcile it.

Require matching `request_id` and typed `status`: `confirmed`, `pending`, `ambiguous`, or `not_dispatched`. Only a valid correlated HTTP-200 `confirmed` permits creation of the exact-amount splits-transfer obligation. `not_dispatched` must include the protocol's refusal type and bounded `retry_after_seconds` and be an immutable gateway tombstone. Store its cooldown before allowing a fresh UUID. Unknown fields that contradict the status fail closed.

Match the reference budgets (150-second POST client budget / 140-second gateway envelope; 15-second GET client budget / 10-second gateway envelope) unless refreshed source specifies a stricter tested contract. A slow request cannot stall invoice-event handling or another wallet's bookkeeping.

On restart, inspect unresolved intents before creating any new attempt. An intent with no response is ambiguous; do not assume a POST was never sent. The same UUID lookup is safe even after a failed local completion/outbox commit.

### 4.2 Inspect and preserve the live physical owner

The HOME coding agent must inventory the deployed owner source, rule ID, item names, request/result payloads, persistence service, actuator expire metadata, manual callers and all automatic callers. Record sanitized source hashes and configured minimum interval/hour cap. Live source may be maintained in `earthship-ui` or another repository; make changes in that source of truth and cross-link commits. Do not overwrite unpublished live work or edit OpenHAB JSONDB directly.

Required owner behavior:

1. Recheck explicit `FeederOverride=OFF` and `LightningGoatsRemoteEnabled=ON` locally immediately before actuation. Missing/NULL/UNDEF/error states block. The extension's earlier check alone is insufficient.
2. Persist a request-specific execution claim before ON. Serialize all callers through one owner and preserve the same UUID through gateway, owner, counter receipt and completion.
3. Preserve the observed pulse duration and independent OFF/expire mechanism. OFF must still occur if the script, gateway or extension stops.
4. Increment the feeding counter at most once and record request-correlated completion durably before publishing success. A changing global counter alone is not proof that this request completed.
5. Same-ID replay returns its record; it never sends a second ON. Crash between admission and completion remains uncertain and blocks replacement activity until reconciled.
6. Define `confirmed` honestly: completed owner actuation sequence and durable receipt. Without a dispenser sensor it does not prove that food physically emerged. Do not label an HTTP ACK or an ON command as completion.

### 4.3 Remove the 32-record operating limit without deleting replay protection

Do not solve `ledger_full` by clearing the String Item, silently dropping old UUIDs, increasing the constant alone, or retiring unresolved IDs. A queue expected to run indefinitely needs indexed per-request durable storage.

Preferred implementation: extend the existing HOME gateway's local SQLite storage with an owner-execution journal, and let the sole OpenHAB owner use a dedicated authenticated loopback-only owner API. This is a private owner callback interface, separate from the WireGuard/public-client router. It reuses the gateway process and local durable database; it does not expose arbitrary SQL or OpenHAB commands.

- `POST /internal/owner/requests/<UUID>/claim`: only a previously admitted gateway request can be claimed; persist one execution grant atomically. Return `execute` exactly once. Subsequent calls return the stored state and can never grant execution again, even if the first response was lost. Lost grant response therefore blocks conservatively rather than risking another pulse.
- `POST /internal/owner/requests/<UUID>/complete`: record durable terminal evidence only from the authenticated owner and only for its granted request. Validate matching UUID, recorded counter transition and timestamps. Exact replay is idempotent; conflicting evidence fails closed.
- `GET /internal/owner/requests/<UUID>`: read authoritative durable record for recovery. A missing record is not evidence that an older owner never fed.
- Persist journal transitions `executing`, `complete`, `ambiguous`, plus operator reconciliation events. Keep one global executing/ambiguous guard and existing interval/cap admission. Make outer gateway confirmation depend on this durable owner record, not the latest result Item.
- Keep gateway admission/refusal records and owner records for the lifetime of this protocol initially. Use indexed tables, disk monitoring and consistent backups. Compaction is a later reviewed feature, not part of this change.
- Keep result Items for notification/compatibility, not the only authoritative history. The owner secret stays on HOME; the extension token cannot call private owner routes. Private routes must not be registered on the externally reachable listener.
- Import all retained legacy UUIDs conservatively before enabling the new path, preserve the old ledger read-only, and disable the old protocol so an old request cannot bypass the new journal. Completed records become no-execute tombstones; every unresolved record blocks activation until reconciled. Do not clear previously full history to make migration pass.

If the live owner already has a tested scalable durable journal with equivalent guarantees, adapt that implementation instead of introducing duplicate storage. Document equivalence against every requirement above. A source-only script claiming persistence is insufficient; verify acknowledgement-loss and restart recovery with the actual persistence backend.

### 4.4 Manual feeding and other callers

Route the extension manual endpoint through the same gateway, with a server-generated UUID and bounded status lookup. Preserve its existing authorization, but remove any override-bypass option from the new physical path. Return pending/ambiguous honestly. Manual requests create no splits-transfer obligation and do not change the wallet balance. Inventory and update Earthship UI/legacy callers to the same owner admission path; pause a bypassing caller until converted. Do not rewrite unrelated UI.

## 5. Exact splits transfers, presentation and upgrade

### CyberHerd and SplitPayments contract

Required path is `herd_wallet → source_wallet → existing SplitPayments targets`. The destination is CyberHerd's `source_wallet` because SplitPayments pays its targets from there. Do not replace the existing member-share computation or send directly to members.

- Add `transfer_herd_amount_to_source(settings: CyberherdSettings, *, amount_sats: int, idempotency_key: str) -> TransferOutcome` in CyberHerd `services/send_splits.py`. Positive bounded amount, canonical distinct wallets owned by the correct user, pinned identity/amount, and durable key uniqueness are required. The extension passes the attempt's snapshotted threshold and `feed:<UUID>:splits`.
- Produce typed `pending`, `settled`, `ambiguous`, `insufficient_funds`, or `failed_before_payment` outcomes. Same key and payload returns the recorded result; key reuse with a changed amount/wallet is an error. Persist resolved wallets on first invocation, so settings changes cannot redirect a pending transfer.
- Create an internal invoice on the source wallet with the exact `amount_sats`, `unit='sat'`, `internal=True`; pay it from the herd wallet using supported LNbits APIs. Check the pinned core's internal-transfer fee and spendable-balance semantics. Do not reduce the splits principal or take unexpected fees from the intended remainder.
- Add a durable CyberHerd transfer record before external steps, record invoice/payment identity before paying, and reconcile the same invoice/hash after reply loss. Cover loss of invoice-creation replies and payment replies; no replacement invoice may be paid while the previous one could have settled. An asyncio lock alone is insufficient.
- Add `split_dispatch_mode`: `wallet_threshold` preserves legacy behavior for unmigrated users; `feeding_confirmed` enables the coordinated mode. In coordinated mode, disable the whole-wallet sweep in `maybe_send_splits_for_user`, coordinator hooks and every other sweep entry point. Keep target updates/member tracking active. The exact-transfer API is explicitly authorized by a confirmed-feeding obligation; document its relationship to the legacy `send_splits_enabled` toggle instead of blindly delegating to it.
- Before activation, both extensions must agree on canonical wallets, supported API and coordinated mode. Missing capability or a conflicting sweep path blocks new physical dispatch. Never fall back to the whole-balance helper. Preserve the old helper for accounts outside coordinated mode only.
- Perform the transfer promptly after feeding confirmation; a failed transfer never reruns the feeder. Hold the cycle until that payment is settled and visible in authoritative LNbits accounting. Subsequent balance checks must use that accounting path rather than cached callback balances or a naive `old_balance - amount` comparison, which breaks when deposits arrive concurrently.
- Verify the installed SplitPayments extension actually processes the internal source-wallet credit. A settled herd-to-source payment and completed member payouts are distinct stages. Never repeat the herd-to-source payment because a downstream member payout failed; SplitPayments owns downstream retries.

### Messages and controls

Payment notifications use fresh wallet balance. Feeding notifications describe one confirmed actuation and the amount owed/transferred to splits, without claiming distribution settled prematurely. Durable message retries cannot call feeder or transfer execution. Pin template/event keys per attempt.

Show wallet balance, raw funded-feed estimate, remainder, current cycle, transfer pending/failed state and cooldown. Provide owner-authenticated pause/resume with backlog preview. Reconciliation records actor/reason/evidence and cannot release a HOME UUID still capable of executing. Manual actions use the same gateway safety path without an automatic paid transfer.

### Upgrade and rollback

1. Inventory all automatic and manual dispatchers, CyberHerd sweep callers, source-wallet SplitPayments targets and existing in-flight transfers. Preserve unpublished live source changes.
2. Pause dispatch, quiesce relevant writers, and back up LNbits wallets/extension state plus HOME/gateway request history. Do not change wallet balances or reconstruct them from historical invoices.
3. Install HOME changes first with harmless canary verification, then CyberHerd exact-transfer capability and coordinated mode, then the extension cycle worker. New queue enablement defaults OFF on upgrade.
4. Resolve any old ambiguous feeding or transfer before activation. An old successful feeding with untransferred funds must not be treated as entirely fresh funding; reconcile its outstanding transfer against real evidence first.
5. Preview the actual current wallet balance, threshold, predicted feed count and remainder. **Existing wallet funds become eligible when enabled.** The operator can move unwanted old funds away before enabling; no synthetic opening credit or backfill is involved.
6. Enable only after operator review of the backlog, cooldown and physical cap. Funds arriving during downtime appear through normal LNbits accounting and are included in the next fresh balance read.
7. Restart recovery first resolves unfinished attempts/transfers; only after settlement and cooldown may remaining wallet funds trigger another feeding.
8. Rollback disables dispatch and preserves all operational/payment records. Do not re-enable the legacy whole-balance dispatcher until every unfinished feed/transfer cycle is reconciled. Never erase owner history, pending invoices or UUIDs to get unblocked.

## 6. Review focus

- Physical success with transfer timeout: wallet may still show the full balance; guard must prevent another feed (Tasks 1–2, 4, 6).
- Lost transfer reply after LNbits already paid: same payment reconciles without another withdrawal or pulse (Tasks 2, 4, 6).
- Concurrent deposits/withdrawals or setting changes: use fresh core balance and immutable active-cycle amount/destination (Tasks 1, 4–6).
- Full owner history, stale result Item and HOME restart: no replay and recoverable durable outcome (Tasks 3, 6).
- Existing funds at upgrade, old in-flight sweep and duplicate receipt callbacks: deliberate backlog activation, no duplicated economic action (Tasks 2, 5–6).

## 7. Implementation tasks

### Task 1 — Durable feed cycles without duplicate balances

**Files:** modify `migrations.py`, `models.py`, `crud.py`; create `feed_models.py`, `feed_crud.py`, `tests/test_feed_cycles.py`. Limit `crud.py` changes to integrating cycle storage and preserving payment-message audit behavior.

**Interfaces:** section 3 types and persistence contracts; use supported core wallet reads for balance.

- [ ] Write/run failing real-database tests for two concurrent workers claiming the same wallet: exactly one active cycle. Test different users/wallets stay isolated and invalid thresholds are rejected.
- [ ] Implement migration `m006_feed_cycles` and guard/attempt/outbox transitions. Do not create a credit table, maintain a copied wallet balance, or alter core monetary rows.
- [ ] Fault-inject between physical-confirmation state and unique transfer-outbox insertion; either both commit or neither does. Replay confirmation creates one obligation. Repeat for settled-transfer/cycle-release commit.
- [ ] Test threshold/source changes do not alter active attempt fields, and pending-wallet reassignment is rejected. Characterize explicit transaction semantics through real supported LNbits adapters on SQLite and PostgreSQL.
- [ ] Run focused tests; commit.

### Task 2 — CyberHerd exact-amount transfer and coordinated sweep mode

**Files in CyberHerd:** `services/send_splits.py`, `services/payment_coordinator.py`, settings models/CRUD/API/UI, `migrations.py`; add durable transfer storage and `tests/test_feeding_confirmed_splits.py`. In this repository add a narrow adapter `services/feed_distribution.py` and `tests/test_feed_distribution.py`.

**Interfaces:** `transfer_herd_amount_to_source` and `TransferOutcome` in section 5; coordinated-mode capability/configuration check.

- [ ] Write/run failing tests with herd wallet 2,340 sats and an exact 1,000-sat request: source-wallet credit is 1,000 and fresh herd balance is 1,340. A second distinct 1,000 request leaves 340; replaying either key changes nothing.
- [ ] Implement durable transfer identity, exact internal invoice amount and authoritative payment reconciliation. Wrong-user wallets, equal wallets, invalid amount and reused key with changed payload fail closed.
- [ ] Fault-inject before/after invoice creation, payment submission and settlement, including lost replies and restart. There must be at most one settled source-wallet credit per key. Empty/insufficient wallet is not successful completion of an obligation.
- [ ] Implement `feeding_confirmed` mode and suppress all coordinated-account whole-wallet sweep entry points, while preserving other users' legacy mode and member/target updates.
- [ ] Verify actual supported LNbits internal-transfer fee/spendable-balance behavior and SplitPayments' processing of the source-wallet event. A failed downstream payout does not cause another herd transfer.
- [ ] Run existing and new CyberHerd suites plus adapter tests; commit and cross-link both source pins.

### Task 3 — Correlated gateway client and HOME owner durability

**Files in this extension:** create `services/feeder_gateway.py`, `tests/test_feeder_gateway.py`; modify settings validation and `services/openhab.py` unknown-state handling. **HOME files:** maintained owner script corresponding to `deploy/openhab/feeder-owner-v2.js`, `src/gateway/{server,store}.rs`, `src/openhab.rs`, their tests and deployment docs. Create `docs/deployment/openhab-wallet-feeding.md` here to link source pins and actual contracts.

**Interfaces:** `FeederGateway.get_safety`, `request_feed(UUID)`, `get_request(UUID)`, `close`; typed outcomes from section 4, plus private owner journal API if needed.

- [ ] Inspect deployed source-of-truth/runtime without actuating; record exact payloads, item names, pulse/expire behavior, persistence, safety/cap and other callers. Preserve existing source changes.
- [ ] Write/run failing client tests for matching confirmed UUID, wrong UUID, bare ACK/204, malformed JSON, pending/ambiguous, timeout, 404, redirects and valid/invalid refusal. Implement typed protocol with no `/runnow` fallback.
- [ ] Write/run failing owner tests for concurrent manual/automatic calls, override changes after precheck, invalid safety, duplicate UUID at every phase, lost execution-grant response, interrupted pulse and failed completion persistence.
- [ ] Implement claim-before-ON and durable correlated recovery per section 4. Preserve physical pulse quantity/OFF backstop, local safety checks and one global owner guard.
- [ ] Complete at least 100 simulated requests, replay the first UUID, restart HOME/gateway and replay it again. Expect exactly 100 ON commands and no 32-entry exhaustion. Test disk/persistence failure blocks new ON without deleting history.
- [ ] Test current result Item overwritten/lost: journal recovers the correct request result. Import legacy IDs as no-execute records and block on unresolved legacy records. Verify private owner routes cannot be accessed with the external extension credential.
- [ ] Run HOME-required format/lint/tests and client suite; commit exact source pins and deployment instructions. Mocks are not proof of actual food delivery.

### Task 4 — Balance-driven worker and recoverable feed/transfer cycle

**Files:** create `services/feeding.py`, `tests/test_feed_worker.py`; modify `tasks.py`, `__init__.py`, `feed_crud.py`.

**Interfaces:** `run_feed_step`/`run_feed_worker` from section 3, consuming Tasks 1–3.

- [ ] Write/run failing integration test starting at 2,340 sats/1,000 threshold. One receipt wakeup yields first feeding, exact transfer, cooldown, second feeding, exact transfer, then idle at 340. No subsequent receipt event is delivered.
- [ ] Implement worker/lifecycle registration and two-second checks. Read fresh core balance for each new cycle; use immutable threshold only for the active attempt. Remove old direct wallet-trigger/distribution branch and in-memory cooldown as authorities.
- [ ] Test confirmed feeding with blocked transfer and still-2,340 balance: repeated worker ticks cause no second ON. Settle its original transfer and observe 1,340, then permit only the next eligible cycle. Test crash after successful payment before cycle release recovers the existing payment first.
- [ ] Test 660 more sats after idle at 340 yields one more complete cycle and zero remainder. Duplicate/stale invoice callbacks cannot duplicate a cycle. Deposits while a transfer is settling are not lost by a cached absolute-balance overwrite.
- [ ] Test withdrawal before POST cancels a never-sent intent; withdrawal after possible POST preserves uncertainty/owed transfer. Test sub-threshold and fractional-msat balances do not trigger and retain remainder.
- [ ] Test override/cap/unavailable safety pause, followed by automatic resume without another payment. Outstanding confirmed-feed transfer can settle while new feed dispatch is paused.
- [ ] Test cancellation at every network/commit boundary, extension stop unregisters all owned tasks/listeners, and restart recovers same UUID/payment. No physical request after stop. Run focused tests; commit.

### Task 5 — Dashboard, notifications and wallet-based upgrade

**Files:** modify `views_api.py`, `models.py`, `static/js/index.js`, `templates/lightning_goats/index.html`, `services/messaging.py`, `README.md`; create `services/feed_outbox.py`, `tests/test_feed_status.py`, `tests/test_feed_migration.py`, `docs/deployment/wallet-feeding-upgrade.md`; update affected existing tests without weakening assertions.

**Interfaces:** status/queue fields, pause/resume/reconciliation in sections 2–5. Existing balance endpoint retains its original meaning.

- [ ] Write/run failing tests showing balance 2,340 → 1,340 → 340 across settled transfers, funded-feed estimates 2 → 1 → 0, remainder 340 and final progress 34%. Pending transfer is visibly blocking even if displayed wallet balance is still high.
- [ ] Implement dashboard/backlog preview, same-user authorization, bounded audited reconciliation and correct stage-specific messages. Retry notifications independently from physical and transfer execution.
- [ ] Test manual feed uses common safety and changes no wallet funds; another user's reconciliation is denied; a still-executable HOME request cannot be released as not-fed.
- [ ] Test migration preserves wallet balances and payment rows, adds only operational state, and defaults queue OFF. Enabling with reviewed 2,340 balance creates two cycles without replaying historic payments or importing credit. Deposits during downtime work after restart.
- [ ] Document coordinated mode before activation, old in-flight feed/sweep reconciliation, backup/pause/enable/rollback procedure and dedicated-wallet expectations. Update README to the new wallet-based behavior only when implemented and tested.
- [ ] Run API/security/UI/migration/message tests; commit.

### Task 6 — End-to-end verification and handoff

**Files:** create `tests/test_wallet_feeding_end_to_end.py`, harmless gateway/owner integration fixtures and `docs/testing/wallet-feeding-acceptance.md`.

- [ ] In a disposable supported LNbits checkout with CyberHerd and SplitPayments installed, record exact environment/setup and source pins. The planning environment initially lacked pytest/LNbits, so this plan claims no executed runtime tests.
- [ ] Run `python -m pytest tests/test_feed_cycles.py tests/test_feed_distribution.py tests/test_feeder_gateway.py tests/test_feed_worker.py tests/test_feed_status.py tests/test_feed_migration.py tests/test_wallet_feeding_end_to_end.py -q`, then the complete extension suite with `python -m pytest tests -q`.
- [ ] Run CyberHerd's new and existing suites, supported SQLite/PostgreSQL paths, and the HOME repository's required Rust format/lint/tests and owner-JavaScript harness.
- [ ] Exercise complete harmless path: actual core wallet balance → one UUID → durable owner completion → one exact internal payment → fresh core balance → next cycle. Inject Review Focus failures and run beyond 32 requests without losing replay protection.
- [ ] Verify old automatic sweep cannot race the coordinated mode and stale core-balance observations cannot release another physical feeding prematurely. Separate herd-to-source settlement from onward recipient success in evidence.
- [ ] Record source pins, commands, results, runtime versions and any live-system limitations. Hand off tested artifacts/runbooks. Operator selects deployment timing and any real physical-test amount; code completion does not silently feed goats.

## 8. Definition of done

- A 2,340-sat LNbits wallet at a 1,000-sat trigger yields exactly two completed feed/transfer cycles, each sending 1,000 sats into the configured splits source wallet, and leaves 340 sats in the herd wallet.
- No duplicate feeding-credit ledger, opening-credit import or invoice-derived balance exists. Wallet balance is the monetary source of truth.
- Backlog drains without new payments; cooldown, override, cap and one-active-cycle guard apply.
- A successful feed with an uncertain transfer cannot feed again; recovery reconciles the original UUID and payment. Duplicate events/restarts cannot duplicate either physical operation or transfer.
- Existing wallet funds are considered deliberately at activation, and competing CyberHerd whole-wallet sweeps are disabled for coordinated accounts.
- OpenHAB results are correlated, durable and recoverable beyond 32 requests without erasing replay protection.
- All affected repositories' relevant tests and the full extension suite pass; source/mock evidence is distinguished from operator-authorized live acceptance.
