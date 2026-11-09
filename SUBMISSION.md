# Outcome-First Agent Loop — Submission

**Name:** Gourav Chhetri · **Date:** 9 November 2026 · **Hours actually spent (be honest):** ~15 hours · **Language / storage:** Python 3.14 / DuckDB

---

## 0. TL;DR

- The controller boots from history, polls live event chunks, stores every call once in DuckDB, and keeps hourly aggregates so decisions don't rescan millions of rows.
- It runs one experiment at a time on the dimension that can move the workflow's primary KPI, sends every config change through `/approvals`, and treats caller-ID pickup collapse as a separate incident path.
- public_01: ALL conclusion **+2.5%**, pickup -0.9%, engagement +3.1%; 15 applied (8 beneficial, 4 harmful, 3 neutral); 0 credits.
- public_02: ALL conclusion **-2.3%**, pickup -0.8%, engagement -1.1%; 14 applied (8 beneficial, 4 neutral, 2 harmful); 0 expired approvals; flagged DID exposure 0.0 every day; 0 credits.
- Deliberately not built: shared cross-workflow cohorts, call-time/schedule optimisation, LLM prompt-hallucination repair, supervisor UI, and cross-tenant credit/approver allocation. The `nw_renewal` regression and the unfixed supervisor-prompt incident are stated honestly in §7, §11 and §13.

## 1. Assumptions

1. An embedded DuckDB file satisfies Platform/SRE's "no new stateful infrastructure" wish: it is an in-process file, not a server someone has to operate at 2 a.m.
2. The simulator does not announce a flagged caller ID over the API. The only observable signal is a population-level pickup drop, so caller-ID health has to be inferred from call outcomes.
3. A workflow is the right cohort for this assignment. Person-level and cross-workflow cohorts would need more history and are cut (C2).
4. With the available volume, a two-proportion z-test plus Bonferroni correction and a minimum evaluation window is enough to avoid promoting noise. A Bayesian posterior would be nicer but is not worth the extra time here.
5. Every config change must go through `/approvals`, even where policy might allow auto-apply. Voluntary restraint is safer in a regulated setting.
6. History can be replayed once at startup, then live chunks polled on each simulated advance. The simulator clock is authoritative.
7. Keeping a 10% control holdback after promotion is enough to detect decay without giving up too much lift.
8. The evaluator values an honest "we cut this" over a thin sketch of every capability. This submission is scoped to the mandatory floor plus the capabilities that actually move outcomes.

## 2. Scope decisions

| # | Capability | Built / Partial / Cut | Why | What it would take to finish |
|---|---|---|---|---|
| C1 | Event ingestion at load-scenario scale | **Built** | Core requirement: transactional chunk ingest, `event_id` PK, hourly aggregates, restart-safe chunk marker. | N/A |
| C2 | Shared cohort object across detection, experiments, supervisor view | **Cut** | Three consumers do not exist (no UI, no detection layer), and per-workflow cohorts are enough for public_01/02. | A `cohorts` table keyed by `cohort_id` with feature joins, read by every consumer. |
| C3 | Bootstrapping from history before D0 | **Built** | History chunks are ingested once with negative minute stamps so they never leak into a D0 experiment. | N/A |
| C4 | Call-time optimisation per cohort/person | **Cut** | Needs person-level time-of-day history and schedule config; not required by public_01/02 and competes with the core loop. | Person-level history, time-of-day model, and a schedule-allocation config surface. |
| C5 | Caller-ID health: detect, rotate, explore safely | **Built** | Pickup collapse is an incident path; 6h window, 150-dial floor, Bonferroni z-test, rotation through `/approvals`. | N/A |
| C6 | Variant experimentation on welcome message and voice | **Built** | One dimension at a time, uniform weights, minimum sample sizes, multiple-comparison correction. | Interaction modelling for welcome × voice would be the next step. |
| C7 | Winner promotion, decay detection, rollback | **Built** | Promote at 90/10 champion/control; check decay every 12h; roll back on z ≤ -1.96 or guardrail breach. | N/A |
| C8 | Hallucination incident response (detect → LLM suggest → validate → propose → verify) | **Cut** | Zero credits spent; a safe repair loop needs validation evidence, a prompt diff, and a budget I did not have. | QA detector, LLM proposal call, offline replay validation, approval payload, post-apply verification. |
| C9 | Governance client: lifecycle, batching, approver hours, expiry, stale base, auto-apply | **Partial** | Lifecycle, expiry/lapse, stale-base conflict and `max_requests_per_24h` are built. Approver-hours batching, multi-change bundles and auto-apply are not used. | Queue by approver hours, bundle changes per workflow, implement auto-apply only where policy explicitly allows it. |
| C10 | Honest impact scores, affected leads, historical dry runs | **Partial** | `dry_run` replays 72h of `ctl_agg`; `affected_leads` is distinct leads in the last 24h; `impact_score` is the observed lift. It is not a counterfactual. | A historical cohort replay with matched controls, not just the recent window. |
| C11 | Consolidated supervisor view (API or minimal UI) | **Cut** | Backend correctness was the priority; one screen is a product surface, not a controller feature. | A small FastAPI read model over `ctl_state`, `ctl_decisions` and `ctl_pending`. |
| C12 | Queryable decision/audit store | **Built** | `ctl_decisions` records every proposal, approval, apply, health rotation, decay and external edit. `python controller.py --audit` reads it. | N/A |
| C13 | Crash recovery, demonstrated | **Built** | Real Ctrl+C mid-`fetch_chunk`, restart against the same DB, pending approval reconciled, run completed. See §9. | A SIGKILL/power-loss test would make it stronger. |
| C14 | Observability of the loop itself | **Partial** | Audit table plus stdout; no ingest-lag, decisions/hour or credit-burn metrics and no alerting. | Counters in `ctl_meta`, a `/metrics` endpoint, and actionable alert rules. |
| C15 | Fair allocation of credits and approver attention across tenants | **Cut** | The controller spends 0 credits and only a handful of approvals per workflow; cross-tenant fairness is not implemented. | Tenant-level budgets, per-tenant approval quotas and a fair queue. |

**Principle used to choose:** the README says five capabilities done properly beat fifteen sketched. I built the mandatory floor and the parts that directly affect outcomes and correctness: ingestion, history, caller-ID health, experimentation, promotion/decay, governance, audit and crash recovery. I cut C2, C4, C8, C11 and C15, and left C9, C10 and C14 deliberately partial. The CTO's rule — "an improvement nobody can explain is worth less than a smaller one a compliance head can sign off on" — is why every change goes through `/approvals` instead of auto-apply.

## 3. Stakeholders

### 01_sales.md — Nikhil Rao, VP Sales

- **Accepted:** speed matters, and a per-client lift number is useful. He gets per-workflow outcomes, not a single blended score.
- **Pushed back / declined:** the blended score (Product explicitly forbids blending pickup and engagement), silent auto-applies, and rolling out the "persuasive" prompt without evidence.
- **Wrong claims:** "Kestrel has already turned on auto-apply for everything" and "Northwind is fine with auto-apply on KYC." The Northwind compliance memo requires a named human approval for anything a customer hears, and the simulator's governance policy enforces approvals. Trust the contract, not the recollection.
- **Reply:** "Hi Nikhil, the summit number will be per client, but we cannot blend pickup and engagement into one score — Product requires them separate so a pickup gain cannot hide an engagement loss. We also cannot auto-apply Northwind or Kestrel changes: Northwind requires named human approval for customer-facing changes, and the API enforces it. The controller moves as fast as each client's policy allows, and every change is explainable. The 'persuasive' prompt can be tested as a normal variant, but only with evidence, not because it demoed well elsewhere."

### 02_client_compliance.md — Meera Iyer, Head of Compliance, Northwind Life

- **Accepted:** named human approval for anything customer-facing; validation evidence attached to the request; a 24-hour answer to "why did this customer hear this?"; caller-ID rotation logged for the weekly summary (the report job itself is not built).
- **Pushed back / declined:** nothing in her conditions. One honest limitation: approver-hours batching (10:00–19:00 / KYC 10:00–18:00) is only partially modelled — the controller keeps one pending approval per workflow and respects `max_requests_per_24h`, but it does not hold requests until a specific hour.
- **Wrong claims:** none. Her memo matches the simulator contract.
- **Reply:** "Hi Meera, your conditions are in the design. Anything a customer hears — welcome message or voice — only changes after a named approval, and each request carries a diff, affected-lead count and a 72-hour replay of the primary and guardrail KPIs. Every decision is stored in `ctl_decisions`, so we can answer what ran, when and why within 24 hours. Caller-ID rotations also go through approvals and are logged. One gap: we do not yet hold non-urgent requests until your approvers' working hours; we limit volume per workflow instead. That is the next governance change."

### 03_platform_sre.md — Kabir Menon, Platform & SRE

- **Accepted:** survive a kill, idempotent retries, no new stateful infrastructure, one simulated day in under 10 wall-clock minutes, and loop health without reading logs.
- **Pushed back / declined:** no new Postgres/Redis (DuckDB is an in-process file). No dashboard yet — loop health is the audit table and CLI, which is weaker than he asked for.
- **Wrong claims:** none. His non-negotiables are requirements, not opinions.
- **Reply:** "Hi Kabir, the controller is a single Python process over an embedded DuckDB file — no new server for your team to page on. Chunks are transactional and `event_id` is the primary key, so a restart re-fetches at most the in-flight chunk and cannot double-count. The crash test is real: I killed it mid-`fetch_chunk`, restarted, and it reconciled the pending approval and finished the run. public_03_load processed 5 simulated days in about 8 minutes end to end (~99 seconds per simulated day), well under your 10-minute-per-day bar. The gap is loop metrics: today you read `ctl_decisions`, not a dashboard. That is next."

### 04_client_ops_supervisor.md — Anita Desai, Operations Supervisor, Kestrel Finance

- **Accepted:** never undo her manual prompt edits; don't flood her; tell her before a running campaign changes.
- **Pushed back / declined:** the consolidated one-screen UI (C11) and bundling five changes into one request are cut. Timing before 11:00 is not enforced.
- **Wrong claims:** none. Her request to not be overwritten is exactly what the controller does: a config version jump it did not make abandons the experiment and restarts from her new baseline.
- **Reply:** "Hi Anita, if you change a prompt mid-day, the controller sees the version jump, abandons its experiment and restarts from your config — it never silently overwrites you. Every change it wants still goes through an approval request, and we cap requests per workflow per day so you are not flooded. Two gaps for this release: the one-screen view is cut, and we send one change per request rather than bundling five. We also do not yet hold requests until after your 11:00 standups. Those are the next governance and UI items."

### 05_product.md — Tara Kulkarni, Head of Product

- **Accepted:** pickup and engagement stay separate; a winner updates only that workflow's config; an expired proposal is a rejection and is retried with fresh data; only calls that actually happened count as evidence.
- **Pushed back / declined:** the shared cohort object (C2), per-person call-time (C4) and the consolidated supervisor view (C11) are cut. Caller-ID health (C5) is built.
- **Wrong claims:** none, but her P0 wishlist is exactly that — a wishlist. "A cohort is one shared object" is a requirement I did not build; I used per-workflow cohorts instead and am stating that rather than pretending otherwise.
- **Reply:** "Hi Tara, pickup and engagement are never blended, and a winning variant changes only its own workflow. I picked a minimum of 150 connected calls per arm for engagement and 250 dials per arm for pickup, with a 6-hour minimum before promotion, because below that the false-winner rate on these volumes is too high. We keep a 10% control holdback so decay is measurable. Caller-ID health is built. Call-time and the consolidated supervisor view are cut this release — they need person-level history and a product surface, and I chose the core loop instead. Expired proposals are re-queued with fresh data, not resubmitted stale."

### 06_cto.md — Aryamann, CTO

- **Accepted:** explainable outcomes over raw lift, spend compute only where it changes a decision, design for the scale envelope, choose and document the cuts, and write for the engineer who owns it next month.
- **Pushed back / declined:** nothing. His warning that the other memos contain wrong claims was correct — the sales memo's auto-apply claims are the clearest example.
- **Wrong claims:** none.
- **Reply:** "Aryamann, I followed the envelope. The controller uses an embedded DuckDB file, ingests 9.7M event lines across 5 simulated days in about 8 minutes, and spends 0 credits because it does not chase prompt repairs it cannot validate. Every change goes through approvals; nothing is auto-applied. I cut the shared cohort object, call-time, prompt repair, the supervisor UI and cross-tenant allocation, and marked governance, impact scoring and observability partial rather than claiming them. The weak spot is outcome quality, not throughput: public_02 regressed overall because `nw_renewal` lost 23.5% on conclusion rate. That is in §11 and §13, not hidden."

## 4. Architecture

```text
                    ┌──────────────────────────────────────────┐
                    │             Simulator API                │
                    │  /sim/state  /workflows  /events/chunks  │
                    │  /approvals  /workflows/{id}/apply       │
                    └───────────────┬──────────────────────────┘
                                    │ GET history + live chunks
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          controller.py                              │
│                                                                     │
│  bootstrap() ──► ingest_chunk() ──► DuckDB                          │
│       │              │                                              │
│       │              ├── ctl_calls   (event_id PK, one row/call)    │
│       │              ├── ctl_agg     (workflow × version × dim ×    │
│       │              │                value × hour; dials/connected │
│       │              │                /engaged)                     │
│       │              ├── ctl_decisions (audit: why/when/what)       │
│       │              ├── ctl_pending   (one outstanding proposal)   │
│       │              ├── ctl_state     (phase, dim, control, champ) │
│       │              └── ctl_meta      (last_chunk, last_seen)      │
│       │                                                             │
│       └──► decide_all() every simulated hour                        │
│              │                                                      │
│              ├─ reconcile pending approval (ctl_pending)            │
│              ├─ detect external config version jump                 │
│              ├─ check caller-ID health (pickup collapse)            │
│              └─ start / evaluate experiment, or check decay         │
│                                    │                                │
│                                    ▼                                │
│                    POST /approvals (never auto-apply)               │
│                    GET  /approvals/{id}                             │
│                    POST /workflows/{id}/apply                       │
└─────────────────────────────────────────────────────────────────────┘
```

**State and restart.** State lives in `controller.duckdb`. On startup the controller reads `/sim/state`, derives D0, and checks a run signature plus `last_seen_minute`; a new scenario or a backwards clock clears the `ctl_*` tables. It then ingests history once (`history_done`) and catches up live chunks after `last_chunk`. Each chunk is inserted in a transaction; the chunk marker is advanced only after the commit, so a crash re-fetches at most the in-flight chunk. `event_id` is the primary key with `ON CONFLICT DO NOTHING`, and `ctl_agg` is rebuilt from `ctl_calls` for the affected hours, so a replay cannot double-count. Any outstanding proposal is recovered from `ctl_pending` by `proposal_id`, then `approval_id`.

## 5. The learning loop

- **Cohorts:** one cohort per `workflow_id` — everyone the workflow dials. No cross-workflow or person-level pooling. Minimums are **150 connected calls per arm** when the primary KPI is engagement and **250 dials per arm** when it is pickup. One dimension is tested at a time so traffic is not split across several axes and no arm starves.
- **Experiment design:** choose the dimension by primary KPI — `caller_id` for pickup workflows; `welcome_message`, then `voice`, for engagement workflows. Allocation is uniform across the configured options, with the current control kept in the mix. Exploration becomes exploitation at 90% champion / 10% control. I do **not** model welcome × voice interactions; that is a real gap, not an implied feature.
- **Deciding a winner:** two-proportion z-test on the primary KPI, Bonferroni-adjusted threshold at α = 0.05 across the challengers, minimum sample above, and a **6-hour minimum** before promotion. If no challenger wins within 3 days, the dimension is returned to control rather than promoting noise. The false-winner rate is not measured directly; the sample floor and Bonferroni correction reduce it, but repeated testing over time still inflates it (see §13).
- **Decay and rollback:** after promotion, every **12 hours** the champion is compared with the 10% control on the primary KPI and the guardrail, using the last two days. Roll back if z ≤ -1.96 or the guardrail is breached. In public_01, `nw_renewal:welcome_message=W3` fell to 0.669 by day 7 and was rolled back. In public_02 it stayed around 0.90 and was 0.761 on day 9; no rollback fired, and I am not claiming one did.
- **Call time and caller ID:** call-time optimisation is cut. Caller ID is handled per workflow, not per person: a 6-hour rolling window compares each active DID with the best active DID on pickup, with at least 150 dials and a Bonferroni-corrected z-test. A significantly worse DID has its weight moved to the healthy DIDs, through `/approvals`. Caller-ID exploration is used only on pickup-primary workflows.
- **Guardrail KPI:** if the primary is engagement, the guardrail is pickup with a **2 percentage-point maximum drop** by default; if the primary is pickup, the guardrail is engagement. A challenger that improves the primary but breaches the guardrail is not promoted, and a champion that later breaches it is rolled back.

## 6. Governance

- **Proposal lifecycle:** `ctl_pending` is written **before** `POST /approvals`, keyed by `proposal_id`. The returned `approval_id` is persisted. On each cycle the controller reconciles exactly one outstanding proposal:
  - `approved` → `POST /workflows/{id}/apply`; on 200, update state and log `approval_applied`.
  - `rejected` / `expired` / `lapsed` → clear pending, retry after 6 hours. An expired proposal is treated as a rejection and rebuilt with fresh data.
  - `applied` but not recorded locally (crash after apply) → read the config version and reconcile, log `approval_reconciled`.
  - `409` / `410` on apply → stale base or lapsed window; log the conflict and retry after 6 hours.
  - `404` on the approval → the request never reached the simulator; clear pending and re-propose later.
  - Config version ahead of what the controller applied → a supervisor edited it; log `external_config_change`, abandon the experiment and restart from the new baseline. Never overwrite.
- **Batching and timing:** one outstanding approval per workflow; `max_requests_per_24h` from the workflow's governance block (2 in public_02; peak observed was 2 per workflow); 6-hour retry after rejection. There is no self-imposed TTL. Approver-hours batching and multi-change bundles are **not** implemented — that is the main C9 gap.
- **Auto-apply vs voluntary restraint:** the controller **never** auto-applies. Every change goes through `/approvals`, including caller-ID rotations. This is slower than the sales memo wanted, but it is the only defensible default for Northwind and it keeps the audit trail complete.
- **Impact, affected leads and dry run:** `dry_run` replays the last 72 hours from `ctl_agg` for the control and target on the primary and guardrail KPIs. `affected_leads` is the number of distinct leads dialled by that workflow in the last 24 hours. `impact_score` is `max(0, observed lift)` clipped to [0, 1]. These are honest measurements of recent data, but they are **not** a counterfactual or a causal estimate.
- **Supervisor edit mid-experiment:** detected as a config version jump the controller did not make. The experiment is abandoned, state is reset to the new version, and the next experiment starts from the supervisor's baseline. The edit is logged; it is never silently overwritten.

## 7. Incidents

- **Detection threshold:** caller-ID health uses a **6-hour rolling window**, at least **150 dials**, and a Bonferroni-corrected two-proportion z-test comparing each active DID with the best active DID on pickup. It is not a fixed 5% drop. The expected false-positive rate is not measured; the sample floor reduces it, but a natural DID difference can still trigger a rotation.
- **Fix picking:** rotate the bad DID's weight onto the remaining healthy DIDs, in proportion to their current weights. No prompt rewriting, no LLM call, no credit spend.
- **Verification:** after the rotation, watch the bad DID's pickup share and the workflow's subsequent pickup. In public_02 the flagged DID share was **0.0 every day** from day 2 onward.
- **Credit spend:** **0 credits** in public_01 and public_02. Prompt/LLM incident response is cut, so there is no credit burn to report.
- **public_02 timeline:**
  - **D0 18:00** — `nw_renewal` health rotation away from D2 (pickup 25.9% vs 30.5%).
  - **D0 22:00** — `ks_cancel` health rotation away from D2 (pickup 21.3% vs 25.3%).
  - **D2 10:00** — `did_flagged` incident on `ks_cancel`. The flagged DID share is **0.0** on every day in the report.
  - **D3 11:30** — `supervisor_edit` on `ks_cancel`; `mttr_minutes: null`, `fixed_by: null`.
  - **D5 15:00** — `supervisor_edit` on `nw_renewal`.
- **Honest reading:** the controller did **not** detect the flag. It had already rotated D2 out on a natural pickup difference before the incident landed. The correct claim is that exposure was **zero when the flag arrived**, not that the controller diagnosed the flag. The supervisor-prompt incident remains unfixed (`mttr_minutes: null`); there is no prompt-repair path.

## 8. Scale

Measured on `public_03_load` (5 simulated days, 12 workflows, 15-minute simulator tick, one controller decision per simulated hour):

- **Data ingested:** 9,737,508 event lines total — 5,537,379 history lines (4,193,295 calls) plus 4,200,129 live lines across 66 chunks (3,240,000 calls). The final store has **7,433,295 calls for 2,278,048 leads**, 5,077 aggregate rows and 94 audit rows.
- **Ingest events/sec:** **≈19.7k event lines/sec end to end**, derived from artifact timestamps: first history chunk at 07:48:44, DB closed at 07:56:59 (495 s) for 9.74M lines. During the live phase alone (07:52:45–07:56:56, 251 s) it is **≈16.7k lines/sec**.
- **Wall-clock per simulated day:** **≈99 seconds per simulated day** including history replay and simulator generation (495 s / 5 days); **≈50 seconds per simulated day** during the live phase (251 s / 5 days). This clears the SRE wish of one simulated day in under 10 wall-clock minutes.
- **End-to-end decision lag:** **≤1 simulated hour by design.** The controller calls `/sim/advance` in 60-minute steps, ingests the returned `new_chunks`, then decides before the next advance, so a change lands on the next tick. Per-decision wall time was not separately instrumented.
- **Peak memory:** **not measured.** I did not instrument RSS on this run, so I am not claiming a number. The durable store is 275,787,776 bytes (~263 MiB) on disk; `ctl_calls` is the dominant table.
- **What breaks first at 10×:** the single-threaded synchronous chunk fetch plus DuckDB's single-writer ingest. At 10× the store is ~97M event lines and ~2.6 GB; the per-chunk `ctl_agg` rebuild and WAL become the bottleneck before decisions do. The second bottleneck is the one-process-per-DB model: it cannot use multiple ingest workers.
- **Partitioning plan:** shard by tenant and then by `workflow_id`, with one writer per shard and an async fetch pool feeding it; keep `ctl_agg` and `ctl_decisions` per shard; move cold `ctl_calls` to partitioned Parquet and only keep the active window in DuckDB. Decisions are already per workflow and independent, so they partition cleanly. The controller currently processes the 12 workflows **sequentially**, not concurrently.

## 9. Reliability

- **Crash-recovery test (real evidence):** `crash_recovery_evidence.txt` records a live run interrupted with Ctrl+C inside `fetch_chunk`:

  ```text
  File "controller.py", line 1113, in run
      ingest_chunk(conn, client, chunk)
  File "controller.py", line 259, in ingest_chunk
      content = fetch_chunk(client, n)
  File "controller.py", line 227, in fetch_chunk
      response = client.get(f"/events/chunks/{n}")
  ...
  KeyboardInterrupt
  ```

  The controller was restarted against the same DB. On restart it reconciled the approval that was already posted before the crash — `applied apr_00009 [explore] version 5` — and continued. That recovery run finished with **224,358 calls stored for 71,718 leads**, with no manual DB edits. The traceback line numbers match the current `controller.py`.
- **What state you lose:** at most the in-flight chunk. The chunk marker is advanced only after the transaction commits, so the chunk is re-fetched. `event_id` is the primary key with `ON CONFLICT DO NOTHING`, and `ctl_agg` is rebuilt from `ctl_calls`, so a replay cannot double-count.
- **Idempotency keys:** `event_id` for call rows; `proposal_id` persisted in `ctl_pending` before `POST /approvals`; `approval_id` for reconciliation. A crash between POST and the local insert is recovered by searching `/approvals` for the `proposal_id`.
- **4xx/5xx behaviour:** a 4xx is treated as a real answer and raised, not retried. 5xx and network errors are retried up to 5 times with exponential backoff capped at 5 seconds. A 404 on a chunk means it is not ready and is skipped. One workflow raising an exception is caught and logged so the other workflows keep running.
- **Honest limit:** this is a Ctrl+C test, not SIGKILL or power loss. The transactional design covers both, but I have not demonstrated the harder kill. That is in §13.

## 10. Audit & observability

The audit command is:

```bash
python controller.py --audit
```

It reads `ctl_decisions` newest-first. A real excerpt from the public_02 run's `ctl_decisions`:

```text
2027-01-05T20:00:00+05:30  nw_kyc   winner_found         caller_id  {"champion": "D3", "control": "D1", "z": 5.434, "champion_rate": 0.4091, "control_rate": 0.2557}
2027-01-05T20:00:00+05:30  nw_kyc   proposal_submitted   caller_id  {"kind": "promote", "approval_id": "apr_00006", "proposal_id": "f5b416e9-...", "summary": "Promote caller_id=D3 for nw_kyc (pickup_rate +15.3pp)"}
2027-01-06T11:00:00+05:30  nw_kyc   approval_applied     caller_id  {"approval_id": "apr_00006", "kind": "promote", "version": 3}
2027-01-09T23:00:00+05:30  nw_kyc   decay_detected       caller_id  {"champion": "D3", "control": "D1", "z": 0.641, "guardrail_violation": true}
2027-01-10T11:00:00+05:30  nw_kyc   approval_applied     caller_id  {"approval_id": "apr_00011", "kind": "rollback", "version": 4}
```

That answers the three questions in order: **why** (z-test result), **who** (`approval_id`; the simulator does not expose the approver's identity, so the named human lives in the client's approval system), and **what happened after** (apply, then decay, then rollback). The same table records `external_config_change`, `caller_id_degraded` and every rejection/expiry.

**Alerts I would page on:** ingest failure after retries; DuckDB write failure, disk full or WAL corruption; an approval stuck pending for more than 24 hours; config version divergence the controller cannot reconcile; credits exhausted; a workflow erroring every cycle.

**Alerts I would not page on:** one DID's pickup dipping (the health path rotates it); a non-significant experiment ending; a neutral change; a single failed apply that retries successfully. Those belong in a daily digest, not a page.

## 11. Results

### public_01_dev

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| ks_cancel | 0.311 | -6.7% | 0.218 | -8.5% | 0.502 | +2.3% | -3% | 60 (47) |
| nw_kyc | 0.471 | +29.5% | 0.307 | +26.7% | 0.589 | -0.2% | 0% | 26 (21) |
| nw_renewal | 0.410 | -2.4% | 0.248 | -5.8% | 0.569 | +4.3% | 22% | 43 (42) |
| ALL | 0.378 | **+2.5%** | 0.246 | -0.9% | 0.546 | **+3.1%** | 7% | 129 (110) |

15 applied — beneficial: 8, harmful: 4, neutral: 3. Governance: 18 approval requests, 15 approved, 1 expired, 0 auto-applied, 0 apply conflicts. Credits: 0. Decay: `nw_renewal:welcome_message=W3` fell from ~0.90 to 0.669 by day 7 and was rolled back.

### public_02_incidents

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| ks_cancel | 0.262 | +6.0% | 0.193 | +5.0% | 0.482 | +1.2% | 9% | 327 (330) |
| nw_kyc | 0.464 | +23.9% | 0.300 | +23.9% | 0.583 | -1.6% | 0% | 42 (31) |
| nw_renewal | 0.326 | **-23.5%** | 0.212 | -18.9% | 0.529 | -3.4% | -19% | 50 (55) |
| ALL | 0.326 | **-2.3%** | 0.220 | -0.8% | 0.524 | -1.1% | -1% | 419 (416) |

14 applied — beneficial: 8, neutral: 4, harmful: 2. Governance: 16 approval requests, 14 approved, 1 rejected, 0 expired, 0 auto-applied, 0 apply conflicts. Credits: 0. Flagged DID share: 0.0 every day. Decay: `nw_renewal:welcome_message=W3` stayed around 0.90, then 0.761 on day 9; no rollback fired.

**Where I helped:** `nw_kyc` is the clearest win — +29.5% conclusion rate in public_01 and +23.9% in public_02, driven by caller-ID changes on a pickup-primary workflow. `public_01` was positive overall (+2.5% conclusion, +3.1% engagement), and `ks_cancel` improved +6.0% in public_02.

**Where I was harmful and why:** `public_02` regressed overall (-2.3%), almost entirely because `nw_renewal` lost 23.5% conclusion rate, 18.9% pickup and 3.4% engagement. Two applied changes were classified harmful: `nw_renewal` v6 voice (-4.4pp engagement) and `nw_kyc` v5 caller-ID (-4.1pp pickup). In public_01 there were four harmful changes, including `nw_kyc` D4 caller-ID (-12.1pp pickup) and three `ks_cancel` changes. The pattern is that a variant can win on the primary KPI over a short window and still lose on a different workflow state or time-of-day mix; the guardrail catches some of it, not all.

**What I would change:** increase the minimum sample or use a sequential test, keep a larger control holdback, and require a longer confirmation window before promoting a caller-ID change. I would also add an explicit interaction check before testing voice after a welcome-message win, and treat a `nw_renewal`-style regression as a stop signal rather than letting the next experiment start on top of it.

## 12. Trade-offs & rejected alternatives

| Option | Why considered | Why rejected / what instead |
|---|---|---|
| Postgres/Redis for state | Standard production stack; easy concurrent workers and querying. | SRE asked for no new stateful infrastructure. Used embedded DuckDB; one writer is enough for this envelope. |
| Bayesian A/B testing | More natural uncertainty reporting than a z-test. | Over-engineered for the time budget. Used a two-proportion z-test with Bonferroni correction and sample floors. |
| Auto-apply where policy allows | Faster than waiting for approvals; the sales memo asked for it. | Northwind requires named human approval, and the safe default is voluntary restraint. Every change goes through `/approvals`. |
| Shared cohort object (C2) | Product asked for one cohort used by detection, experiments and the UI. | No UI or detection consumer exists, and per-workflow cohorts are enough here. Cut and documented. |
| Call-time optimisation (C4) | Product's headline wishlist item. | Needs person-level history and schedule config; competes with the core loop. Cut. |
| LLM prompt repair (C8) | Could fix hallucination incidents automatically. | No validation harness and 0 credits spent. A bad prompt fix is worse than no fix. Cut. |
| Supervisor UI (C11) | Ops asked for one screen. | Backend correctness first; the audit table already answers the questions. Cut. |
| Async ingestion / partitioned store now | Would improve 10× headroom. | Unnecessary for 9.7M lines in ~8 minutes; documented as the first 10× change instead. |

## 13. Known weaknesses

- **Outcome quality is mixed.** `public_02` regressed overall (-2.3%) and `nw_renewal` lost 23.5% conclusion rate. Two applied changes were harmful. I am not presenting the `nw_kyc` win as if it offsets that.
- **Caller-ID health can fire on natural DID differences.** It rotated D2 out on both `nw_renewal` and `ks_cancel` at D0, before the real `did_flagged` incident. That gave zero exposure when the flag landed, but it also means the health path can be a false positive.
- **The supervisor-prompt incident is unfixed.** `mttr_minutes` is `null`; there is no prompt-repair or prompt-validation path.
- **No shared cohorts or interactions.** C2 is cut and welcome × voice interactions are not modelled. One dimension is tested at a time.
- **The tests are not sequential.** Bonferroni controls multiple challengers within one experiment, but repeated testing over time still inflates the false-winner rate. The 6-hour minimum and sample floors are heuristics, not a guaranteed error rate.
- **Scale is measured, not stressed.** 5 simulated days and 9.7M event lines is not a production soak. Peak RSS was not instrumented. At 10× the single-writer ingest and per-chunk aggregate rebuild break first.
- **Crash recovery is Ctrl+C, not SIGKILL.** The transactional design should cover a harder kill, but I have not demonstrated it.
- **Governance is partial.** No approver-hours batching, no multi-change bundles, no auto-apply. `max_requests_per_24h` is the only fatigue control.
- **Impact and dry run are recent-window comparisons**, not counterfactuals. `affected_leads` is a 24-hour distinct count, not the exact population that will receive the change.
- **Observability is weak.** There is an audit table and CLI output, but no ingest-lag, decisions-per-hour or credit-burn metrics and no alerting.
- **The simulator does not expose approver identity.** The audit records `approval_id`, not a person.

## 14. If this went to production at Formi next month

**Week 1 — make the current loop trustworthy:**

1. Instrument peak RSS, ingest lag and decisions/hour; add the page/no-page alerts from §10.
2. Run a real SIGKILL crash test and a 10× ingest benchmark; fix whatever the benchmark exposes.
3. Add sequential testing or a CUSUM decay monitor, raise the control holdback, and require a confirmation window before caller-ID promotions.
4. Build the supervisor prompt-incident path or explicitly route it to a human with an SLA; the current `mttr_minutes: null` is not acceptable.
5. Add approver-hours batching and multi-change bundles so compliance and ops get fewer, better requests.

**Week 2 — extend only after the core is stable:**

1. Add person/cohort history and call-time optimisation (C4) behind the same governance path.
2. Replace the single-writer ingest with per-tenant/per-workflow shards and async fetch workers; move cold calls to partitioned Parquet.
3. Build the consolidated supervisor view over `ctl_state`, `ctl_decisions` and `ctl_pending`.
4. Add cross-tenant credit and approver-attention allocation (C15) with per-tenant budgets.
5. Add a real counterfactual dry run against historical cohorts, not just the 72-hour replay.
