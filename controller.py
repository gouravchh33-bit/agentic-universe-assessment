"""Outcome-first controller for the Formi agentic-universe simulator.

Design in one paragraph
-----------------------
Read history once, then poll the simulator tick by tick, store every call
idempotently in DuckDB, and keep hourly aggregates so decisions never rescan
the whole event history.  For each workflow, run one statistically-gated
experiment at a time on a dimension that can actually move its primary KPI
(caller id for pickup, welcome message / voice for engagement).  Caller-ID
health is monitored separately on pickup rate, because a flagged number is an
incident, not a normal experiment.  Every config change goes through
`/approvals` (never a silent overwrite), is reconciled after a crash from the
pending-approval table, and is logged to an audit table.  Champions are
promoted only when the primary KPI beats the control with a two-proportion
z-test and the guardrail is respected; they are rolled back if they later
decay relative to the 10% control holdback.

Run:
    ./run.sh http://127.0.0.1:8700
Audit:
    python controller.py --audit
"""
from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from statistics import NormalDist

import duckdb
import httpx

# --------------------------------------------------------------------------- config
SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://127.0.0.1:8700")
DB_FILE = os.getenv("CONTROLLER_DB", "controller.duckdb")

# D0 is the instant the simulator calls minute 0.  It is replaced at startup
# with a value derived from /sim/state; the fallback only documents the public
# simulator's schedule.
D0 = datetime.fromisoformat("2027-01-04T00:00:00+05:30")

ADVANCE_MINUTES = 60          # one decision cycle per simulated hour
MIN_CONNECTED_PER_ARM = 150   # engagement is measured on connected calls
MIN_DIALS_PER_ARM = 250       # pickup is measured on dials
MIN_EXPERIMENT_MINUTES = 6 * 60
MAX_EXPERIMENT_MINUTES = 3 * 24 * 60
DECAY_CHECK_MINUTES = 12 * 60
RETRY_AFTER_REJECTION = 6 * 60
ALPHA = 0.05
DECAY_Z = 1.96

# Caller-ID health: a flagged number usually shows up as a sharp pickup drop.
# We compare each active DID with the best active DID over a rolling window.
CALLER_HEALTH_WINDOW_MINUTES = 6 * 60
CALLER_HEALTH_MIN_DIALS = 150

STATE_COLS = [
    "phase", "dim", "dim_index", "control_value", "champion_value",
    "applied_version", "started_minute", "promoted_minute",
    "last_check_minute", "retry_after_minute",
]


# --------------------------------------------------------------------------- database
def connect() -> duckdb.DuckDBPyConnection:
    """Open the DB and create the controller's own tables.

    Tables are prefixed `ctl_` so an old controller.duckdb (with raw_events /
    system_state) does not collide with the new schema.
    """
    conn = duckdb.connect(DB_FILE)
    conn.execute("CREATE TABLE IF NOT EXISTS ctl_meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ctl_calls (
            event_id BIGINT PRIMARY KEY,
            sim_minute BIGINT,
            ts VARCHAR,
            workflow_id VARCHAR,
            campaign_id VARCHAR,
            lead_id VARCHAR,
            person_id VARCHAR,
            cohort_id VARCHAR,
            config_version BIGINT,
            variant_welcome VARCHAR,
            variant_voice VARCHAR,
            variant_caller VARCHAR,
            prompt_version VARCHAR,
            connected BOOLEAN,
            engaged BOOLEAN,
            disposition VARCHAR,
            hallucination_flag BOOLEAN,
            flow_adherence DOUBLE
        )
    """)
    # One row per (workflow, config version, dimension, value, hour).  Decision
    # queries read this small table instead of scanning millions of calls.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ctl_agg (
            workflow_id VARCHAR,
            config_version BIGINT,
            dim VARCHAR,
            value VARCHAR,
            sim_hour BIGINT,
            dials BIGINT,
            connected BIGINT,
            engaged BIGINT,
            PRIMARY KEY (workflow_id, config_version, dim, value, sim_hour)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ctl_decisions (
            id VARCHAR PRIMARY KEY,
            sim_minute BIGINT,
            ts VARCHAR,
            workflow_id VARCHAR,
            action VARCHAR,
            dim VARCHAR,
            detail VARCHAR
        )
    """)
    # One outstanding approval per workflow.  Persisting it is what makes a
    # restart reconcile instead of losing or double-applying a change.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ctl_pending (
            workflow_id VARCHAR PRIMARY KEY,
            approval_id VARCHAR,
            proposal_id VARCHAR,
            kind VARCHAR,
            payload VARCHAR,
            created_minute BIGINT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ctl_state (
            workflow_id VARCHAR PRIMARY KEY,
            phase VARCHAR,
            dim VARCHAR,
            dim_index INTEGER,
            control_value VARCHAR,
            champion_value VARCHAR,
            applied_version BIGINT,
            started_minute BIGINT,
            promoted_minute BIGINT,
            last_check_minute BIGINT,
            retry_after_minute BIGINT
        )
    """)
    return conn


def meta_get(conn, key):
    row = conn.execute("SELECT value FROM ctl_meta WHERE key = ?", [key]).fetchone()
    return row[0] if row else None


def meta_set(conn, key, value):
    conn.execute("DELETE FROM ctl_meta WHERE key = ?", [key])
    conn.execute("INSERT INTO ctl_meta VALUES (?, ?)", [key, str(value)])


def get_state(conn, wid):
    row = conn.execute(
        f"SELECT {', '.join(STATE_COLS)} FROM ctl_state WHERE workflow_id = ?", [wid]
    ).fetchone()
    return dict(zip(STATE_COLS, row))


def update_state(conn, wid, **fields):
    assignments = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(
        f"UPDATE ctl_state SET {assignments} WHERE workflow_id = ?",
        [*fields.values(), wid],
    )


def ensure_state(conn, wid):
    if not conn.execute("SELECT 1 FROM ctl_state WHERE workflow_id = ?", [wid]).fetchone():
        conn.execute("INSERT INTO ctl_state VALUES (?, 'idle', NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", [wid])


def reset_if_new_run(conn, state):
    """Wipe our tables when the simulator is a different scenario/new world.

    `last_seen_minute` catches the case where the same scenario is re-run from
    D0 on the same DB: sim time goes backwards, so the old state is stale.
    """
    signature = f"{state['scenario']}|{state['end_minute']}|{state['tick_minutes']}"
    stored_signature = meta_get(conn, "run_signature")
    stored_seen = meta_get(conn, "last_seen_minute")
    seen = int(stored_seen) if stored_seen is not None else None
    new_world = stored_signature != signature or (seen is not None and seen > state["sim_minute"])
    if new_world:
        for table in ("ctl_calls", "ctl_agg", "ctl_decisions", "ctl_pending", "ctl_state", "ctl_meta"):
            conn.execute(f"DELETE FROM {table}")
        meta_set(conn, "run_signature", signature)
    meta_set(conn, "last_seen_minute", state["sim_minute"])
    return new_world


# --------------------------------------------------------------------------- HTTP helpers
def get_json(client, path, **params):
    last = None
    for attempt in range(5):
        try:
            response = client.get(path, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            # 4xx is a real answer, not a transient failure.
            if exc.response.status_code < 500:
                raise
            last = exc
        except httpx.HTTPError as exc:
            last = exc
        time.sleep(min(2 ** attempt, 5))
    raise last


def fetch_chunk(client, n):
    for attempt in range(5):
        try:
            response = client.get(f"/events/chunks/{n}")
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.content
        except httpx.HTTPError:
            if attempt == 4:
                raise
            time.sleep(min(2 ** attempt, 5))
    return None


def ts_to_minute(ts):
    return int((datetime.fromisoformat(ts) - D0).total_seconds() // 60)


# --------------------------------------------------------------------------- ingestion
def log_decision(conn, now, ts, wid, action, dim, detail):
    conn.execute(
        "INSERT INTO ctl_decisions VALUES (?, ?, ?, ?, ?, ?, ?)",
        [str(uuid.uuid4()), now, ts, wid, action, dim, json.dumps(detail, default=str)],
    )


def ingest_chunk(conn, client, chunk):
    """Insert one NDJSON chunk exactly once and refresh its hourly aggregates.

    `event_id` is the primary key, so re-fetching after a crash is safe.  The
    chunk marker is only advanced after the transaction commits; if we die in
    between, the next run re-fetches and dedupes.
    """
    n = chunk["chunk"]
    content = fetch_chunk(client, n)
    if content is None:
        return

    fd, path = tempfile.mkstemp(prefix=f"ctl_chunk_{n}_", suffix=".ndjson")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
        quoted = path.replace("'", "''")
        # The simulator flushes a chunk at least once per advance and records
        # the clock value in until_minute.  History chunks are stamped with
        # their real (negative) minute so each chunk rebuilds a different hour;
        # the final history flush at D0 is forced to -1 so it can never leak
        # into an experiment that starts at D0.
        chunk_minute = int(chunk["until_minute"])
        if chunk.get("kind") == "history" and chunk_minute >= 0:
            chunk_minute = -1
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(f"""
                INSERT INTO ctl_calls
                SELECT
                    CAST(json_extract_string(t.json, '$.event_id') AS BIGINT),
                    {chunk_minute},
                    json_extract_string(t.json, '$.ts'),
                    json_extract_string(t.json, '$.workflow_id'),
                    json_extract_string(t.json, '$.campaign_id'),
                    json_extract_string(t.json, '$.lead_id'),
                    json_extract_string(t.json, '$.person_id'),
                    json_extract_string(t.json, '$.cohort_id'),
                    CAST(json_extract_string(t.json, '$.config_version') AS BIGINT),
                    json_extract_string(t.json, '$.variant.welcome_message'),
                    json_extract_string(t.json, '$.variant.voice'),
                    json_extract_string(t.json, '$.variant.caller_id'),
                    json_extract_string(t.json, '$.variant.prompt_version'),
                    CAST(json_extract_string(t.json, '$.connected') AS BOOLEAN),
                    CAST(json_extract_string(t.json, '$.engaged') AS BOOLEAN),
                    json_extract_string(t.json, '$.disposition'),
                    CAST(json_extract_string(t.json, '$.qa.hallucination_flag') AS BOOLEAN),
                    CAST(json_extract_string(t.json, '$.qa.flow_adherence') AS DOUBLE)
                FROM read_json_objects('{quoted}') t
                WHERE json_extract_string(t.json, '$.type') = 'call'
                ON CONFLICT DO NOTHING
            """)

            # Rebuild the affected hours from ctl_calls (not from the chunk), so
            # a replayed chunk cannot double-count.
            bounds = conn.execute(
                "SELECT MIN(sim_minute), MAX(sim_minute) FROM ctl_calls "
                "WHERE event_id BETWEEN ? AND ?",
                [chunk["first_event_id"], chunk["last_event_id"]],
            ).fetchone()
            if bounds and bounds[0] is not None:
                h0 = bounds[0] // 60
                h1 = bounds[1] // 60
                conn.execute("DELETE FROM ctl_agg WHERE sim_hour BETWEEN ? AND ?", [h0, h1])
                conn.execute("""
                    INSERT INTO ctl_agg
                    SELECT workflow_id, config_version, 'welcome_message', variant_welcome,
                           CAST(floor(sim_minute / 60.0) AS BIGINT),
                           COUNT(*),
                           SUM(CASE WHEN connected THEN 1 ELSE 0 END),
                           SUM(CASE WHEN engaged THEN 1 ELSE 0 END)
                    FROM ctl_calls
                    WHERE sim_minute BETWEEN ? AND ? AND variant_welcome IS NOT NULL
                    GROUP BY 1, 2, 3, 4, 5
                    UNION ALL
                    SELECT workflow_id, config_version, 'voice', variant_voice,
                           CAST(floor(sim_minute / 60.0) AS BIGINT),
                           COUNT(*),
                           SUM(CASE WHEN connected THEN 1 ELSE 0 END),
                           SUM(CASE WHEN engaged THEN 1 ELSE 0 END)
                    FROM ctl_calls
                    WHERE sim_minute BETWEEN ? AND ? AND variant_voice IS NOT NULL
                    GROUP BY 1, 2, 3, 4, 5
                    UNION ALL
                    SELECT workflow_id, config_version, 'caller_id', variant_caller,
                           CAST(floor(sim_minute / 60.0) AS BIGINT),
                           COUNT(*),
                           SUM(CASE WHEN connected THEN 1 ELSE 0 END),
                           SUM(CASE WHEN engaged THEN 1 ELSE 0 END)
                    FROM ctl_calls
                    WHERE sim_minute BETWEEN ? AND ? AND variant_caller IS NOT NULL
                    GROUP BY 1, 2, 3, 4, 5
                """, [h0 * 60, h1 * 60 + 59] * 3)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

    previous = int(meta_get(conn, "last_chunk") or "-1")
    meta_set(conn, "last_chunk", max(previous, n))


def bootstrap(conn, client, state):
    """Load history once, then catch up any live chunks already produced."""
    if meta_get(conn, "history_done") != "1":
        if os.getenv("INGEST_HISTORY", "1") != "0":
            for chunk in get_json(client, "/events/chunks", kind="history"):
                ingest_chunk(conn, client, chunk)
        meta_set(conn, "history_done", "1")

    last = int(meta_get(conn, "last_chunk") or "-1")
    for chunk in get_json(client, "/events/chunks", kind="events", after=last):
        ingest_chunk(conn, client, chunk)


# --------------------------------------------------------------------------- statistics
def z_threshold(challengers):
    """Bonferroni-adjusted two-sided 95% threshold for k comparisons."""
    return NormalDist().inv_cdf(1 - ALPHA / (2 * max(1, challengers)))


def two_proportion_z(s1, n1, s2, n2):
    if n1 <= 0 or n2 <= 0:
        return 0.0
    p1, p2 = s1 / n1, s2 / n2
    pooled = (s1 + s2) / (n1 + n2)
    se = (pooled * (1 - pooled) * (1 / n1 + 1 / n2)) ** 0.5
    if se == 0:
        return 0.0
    return (p2 - p1) / se


def metric_parts(row, metric):
    """Return (successes, denominator, rate) for a KPI."""
    if row is None:
        return 0, 0, 0.0
    if metric == "engagement_rate":
        n = int(row["connected"])
        s = int(row["engaged"])
    else:  # pickup_rate
        n = int(row["dials"])
        s = int(row["connected"])
    return s, n, (s / n if n else 0.0)


def variant_stats(conn, wid, dim, since_minute, min_version=0):
    rows = conn.execute("""
        SELECT value,
               SUM(dials), SUM(connected), SUM(engaged)
        FROM ctl_agg
        WHERE workflow_id = ? AND dim = ? AND config_version >= ?
          AND sim_hour * 60 >= ?
        GROUP BY value
    """, [wid, dim, int(min_version or 0), int(since_minute)]).fetchall()
    return {
        value: {"dials": int(dials), "connected": int(connected), "engaged": int(engaged)}
        for value, dials, connected, engaged in rows
    }


def primary_and_guardrail(workflow):
    objective = workflow.get("objective") or {}
    primary = objective.get("primary", "engagement_rate")
    guard = objective.get("guardrail") or {}
    if primary == "engagement_rate":
        return primary, "pickup_rate", float(guard.get("pickup_rate_max_drop", 0.02))
    return primary, "engagement_rate", float(guard.get("engagement_rate_max_drop", 0.02))


def experiment_dims(workflow):
    """Dimensions that can actually move the workflow's primary KPI.

    Caller ID determines whether the phone is answered; welcome message and
    voice only matter after the call connects.  Spending an experiment on
    welcome_message for a pickup-rate workflow (or on caller_id for an
    engagement workflow) cannot produce a real winner, so we do not do it.
    """
    options = workflow.get("options") or {}
    primary = (workflow.get("objective") or {}).get("primary", "engagement_rate")
    order = ["caller_id"] if primary == "pickup_rate" else ["welcome_message", "voice"]
    return [dim for dim in order if len(options.get(dim, [])) > 1]


# --------------------------------------------------------------------------- config helpers
def distribution(config, dim):
    return (config.get("allocations", {}).get("default", {}) or {}).get(dim, {}) or {}


def set_dim_allocation(config, dim, weights):
    """Set a dimension everywhere, including cohort overrides.

    If a cohort has its own distribution for this dimension, leaving it alone
    would mean the experiment only reaches part of the population.
    """
    new = copy.deepcopy(config)
    allocations = new.setdefault("allocations", {})
    allocations.setdefault("default", {})[dim] = dict(weights)
    for key, dims in allocations.items():
        if key != "default" and isinstance(dims, dict) and dim in dims:
            dims[dim] = dict(weights)
    return new


def normalise_weights(weights):
    total = float(sum(weights.values()))
    if total <= 0:
        return {}
    return {option: weight / total for option, weight in weights.items() if weight > 0}


def effective_distribution(config, dim, key="default"):
    """The distribution a cohort really gets: its override, else the default.

    The simulator normalises weights (1.0/1.0 becomes 0.5/0.5) before it
    compares configs, so we must compare normalised effective distributions
    too.  Otherwise we submit a "no change" proposal and the API rejects it.
    """
    allocations = config.get("allocations") or {}
    if key != "default":
        override = allocations.get(key) or {}
        if isinstance(override, dict) and dim in override:
            return normalise_weights(override[dim] or {})
    return normalise_weights((allocations.get("default") or {}).get(dim) or {})


def same_dim_allocation(config, new_config, dim):
    keys = set(config.get("allocations") or {}) | set(new_config.get("allocations") or {})
    return all(
        effective_distribution(config, dim, key) == effective_distribution(new_config, dim, key)
        for key in keys
    )


def recent_leads(conn, wid, now):
    row = conn.execute(
        "SELECT COUNT(DISTINCT lead_id) FROM ctl_calls "
        "WHERE workflow_id = ? AND sim_minute >= ?",
        [wid, now - 24 * 60],
    ).fetchone()
    return int(row[0] or 0)


def caller_health_candidate(conn, workflow, config, now):
    """Find an active caller ID whose pickup rate is significantly worse.

    The simulator does not announce a flagged DID over the API; the only
    observable signal is that its pickup rate falls.  We compare each DID with
    the best active DID over a rolling window, with a minimum sample size and
    a Bonferroni-corrected z-test so one noisy hour cannot trigger a rotation.
    """
    wid = workflow["workflow_id"]
    since = max(0, now - CALLER_HEALTH_WINDOW_MINUTES)
    stats = variant_stats(conn, wid, "caller_id", since, 0)

    active = set()
    for dims in (config.get("allocations") or {}).values():
        if isinstance(dims, dict) and "caller_id" in dims:
            active |= set(dims["caller_id"])
    if not active:
        active = set(distribution(config, "caller_id"))

    usable = {}
    for value, row in stats.items():
        if value not in active:
            continue
        dials = int(row["dials"])
        connected = int(row["connected"])
        if dials >= CALLER_HEALTH_MIN_DIALS:
            usable[value] = (connected, dials, connected / dials)

    if len(usable) < 2:
        return None

    best = max(usable, key=lambda value: usable[value][2])
    best_s, best_n, best_rate = usable[best]
    threshold = z_threshold(max(1, len(usable) - 1))

    worst = None
    for value, (successes, dials, rate) in usable.items():
        if value == best:
            continue
        z = two_proportion_z(successes, dials, best_s, best_n)
        if z >= threshold and (worst is None or z > worst[0]):
            worst = (z, value, rate, dials)

    if worst is None:
        return None
    z, bad, bad_rate, bad_n = worst
    return {
        "bad": bad,
        "best": best,
        "bad_rate": bad_rate,
        "best_rate": best_rate,
        "bad_n": bad_n,
        "best_n": best_n,
        "z": z,
    }


def rotate_away_from_caller(config, workflow, bad):
    """Move the bad DID's weight onto the other DIDs, keeping their ratio."""
    current = distribution(config, "caller_id")
    remaining = {value: weight for value, weight in current.items()
                 if value != bad and weight > 0}
    if remaining:
        weights = normalise_weights(remaining)
    else:
        options = [value for value in workflow["options"]["caller_id"] if value != bad]
        if not options:
            return None, None
        weights = {value: 1.0 for value in options}
    new_config = set_dim_allocation(config, "caller_id", weights)
    if same_dim_allocation(config, new_config, "caller_id"):
        return None, None
    return new_config, weights


# --------------------------------------------------------------------------- approvals
def approvals_last_24h(client, wid, now):
    try:
        approvals = get_json(client, "/approvals", workflow_id=wid)
    except httpx.HTTPError:
        return 0
    return sum(1 for a in approvals if ts_to_minute(a["created_at"]) > now - 24 * 60)


def submit_change(conn, client, now, ts, workflow, old_config, new_config, base_version,
                  dim, kind, payload, summary, rationale, metric=None):
    """Propose one change through governance and persist the pending approval."""
    wid = workflow["workflow_id"]
    primary, guardrail_metric, _ = primary_and_guardrail(workflow)
    metric = metric or primary
    control = payload.get("control")
    target = payload.get("champion") if kind in ("promote", "health") else control

    stats = variant_stats(conn, wid, dim, now - 72 * 60, 0)
    c_s, c_n, c_rate = metric_parts(stats.get(control), metric)
    t_s, t_n, t_rate = metric_parts(stats.get(target), metric)
    lift = t_rate - c_rate if c_n and t_n else 0.0

    diff = {
        f"allocations.default.{dim}": {
            "from": distribution(old_config, dim),
            "to": distribution(new_config, dim),
        }
    }
    dry_run = {
        "method": "replay_recent_calls",
        "window_hours": 72,
        "primary": metric,
        "control": control,
        "target": target,
        "control_rate": round(c_rate, 4),
        "target_rate": round(t_rate, 4),
        "control_n": c_n,
        "target_n": t_n,
        "guardrail": guardrail_metric,
    }
    body = {
        "proposal_id": str(uuid.uuid4()),
        "workflow_id": wid,
        "base_version": base_version,
        "proposed_config": new_config,
        "change_summary": summary,
        "diff": diff,
        "affected_leads": recent_leads(conn, wid, now),
        "dry_run": dry_run,
        "impact_score": max(0.0, min(1.0, lift)),
        "rationale": rationale,
    }
    # Persist the proposal *before* the network call.  If we die between the
    # POST and the local insert, the next run finds the approval by proposal_id
    # instead of losing it (or re-proposing the same change).
    conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
    conn.execute(
        "INSERT INTO ctl_pending VALUES (?, ?, ?, ?, ?, ?)",
        [wid, None, body["proposal_id"], kind, json.dumps(payload), now],
    )
    response = client.post("/approvals", json=body)
    if response.status_code == 200:
        approval = response.json()
        conn.execute(
            "UPDATE ctl_pending SET approval_id = ? WHERE workflow_id = ?",
            [approval["approval_id"], wid],
        )
        log_decision(conn, now, ts, wid, "proposal_submitted", dim,
                     {"kind": kind, "approval_id": approval["approval_id"],
                      "proposal_id": body["proposal_id"], "summary": summary,
                      "lift": round(lift, 4), "impact_score": body["impact_score"]})
        print(f"  proposal {approval['approval_id']} [{kind}] {summary}")
        return True

    conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
    log_decision(conn, now, ts, wid, "proposal_failed", dim,
                 {"kind": kind, "status": response.status_code, "body": response.text[:500]})
    print(f"  proposal failed {wid} [{kind}] {response.status_code}: {response.text[:200]}")
    return False


def apply_transition(conn, now, wid, kind, payload, version):
    """Update workflow state after an approved change is applied."""
    state = get_state(conn, wid)
    if kind == "explore":
        update_state(
            conn, wid,
            phase="exploring", dim=payload["dim"], control_value=payload["control"],
            champion_value=None, applied_version=version, started_minute=now,
            promoted_minute=None, last_check_minute=now, retry_after_minute=None,
            dim_index=int(state["dim_index"]) + 1,
        )
    elif kind == "promote":
        update_state(
            conn, wid,
            phase="exploit", champion_value=payload["champion"],
            control_value=payload["control"], applied_version=version,
            promoted_minute=now, last_check_minute=now, retry_after_minute=None,
        )
    elif kind in ("abandon", "rollback"):
        update_state(
            conn, wid,
            phase="idle", dim=None, champion_value=None, applied_version=version,
            retry_after_minute=None,
        )
    elif kind == "health":
        # A caller-ID rotation is orthogonal to a welcome/voice experiment, so
        # keep that experiment state and only advance the applied version.  If
        # the workflow was idle it stays idle and the next cycle starts from
        # the healthier allocation.
        update_state(
            conn, wid, applied_version=version, last_check_minute=now,
            retry_after_minute=None,
        )


def check_caller_health(conn, client, now, ts, workflow, config, version, state):
    """Rotate away from a caller ID whose pickup rate has collapsed.

    This is incident response, not winner selection: the evidence is a
    population-level pickup drop against the other active DIDs, and the
    change still goes through `/approvals`.
    """
    # A caller_id experiment is already measuring this dimension and changing
    # the allocation underneath it would invalidate the experiment.
    if state["dim"] == "caller_id":
        return False

    candidate = caller_health_candidate(conn, workflow, config, now)
    if candidate is None:
        return False

    new_config, weights = rotate_away_from_caller(config, workflow, candidate["bad"])
    if new_config is None:
        return False

    wid = workflow["workflow_id"]
    bad = candidate["bad"]
    best = candidate["best"]
    log_decision(conn, now, ts, wid, "caller_id_degraded", "caller_id",
                 {"bad": bad, "best": best,
                  "bad_pickup": round(candidate["bad_rate"], 4),
                  "best_pickup": round(candidate["best_rate"], 4),
                  "z": round(candidate["z"], 3), "n": candidate["bad_n"]})

    max_requests = int(workflow["governance"].get("max_requests_per_24h", 4))
    if approvals_last_24h(client, wid, now) >= max_requests:
        log_decision(conn, now, ts, wid, "caller_rotation_deferred", "caller_id",
                     {"reason": "max_requests_per_24h", "limit": max_requests, "bad": bad})
        return False

    summary = (f"Rotate caller_id away from {bad} for {wid} "
               f"(pickup {candidate['bad_rate']:.1%} vs {candidate['best_rate']:.1%})")
    rationale = (
        f"{bad} answered only {candidate['bad_rate'] * 100:.1f}% of dials over the last "
        f"{CALLER_HEALTH_WINDOW_MINUTES // 60}h, versus {candidate['best_rate'] * 100:.1f}% for "
        f"{best} (z={candidate['z']:.2f}, n={candidate['bad_n']}). That is a population-level "
        f"pickup collapse, so we stop dialling from the unhealthy number and move its share "
        f"to the healthy caller IDs."
    )
    payload = {"dim": "caller_id", "control": bad, "champion": best,
               "bad": bad, "best": best, "new_weights": weights}
    return submit_change(
        conn, client, now, ts, workflow, config, new_config, version, "caller_id",
        "health", payload, summary, rationale, metric="pickup_rate",
    )


def handle_pending(conn, client, now, ts, wid):
    """Reconcile the one outstanding approval for a workflow, if any."""
    row = conn.execute(
        "SELECT approval_id, proposal_id, kind, payload FROM ctl_pending WHERE workflow_id = ?", [wid]
    ).fetchone()
    if not row:
        return False
    approval_id, proposal_id, kind, payload_json = row
    payload = json.loads(payload_json)

    if approval_id is None:
        # We crashed between POST /approvals and recording its id.  Find our
        # request by the proposal_id we generated before the call.
        try:
            approvals = get_json(client, "/approvals", workflow_id=wid)
        except httpx.HTTPError:
            return True
        match = next((a for a in approvals if a["proposal_id"] == proposal_id), None)
        if match is None:
            # The request never reached the simulator; safe to retry later.
            conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
            log_decision(conn, now, ts, wid, "proposal_recovered_missing", payload.get("dim"),
                         {"proposal_id": proposal_id})
            return True
        approval_id = match["approval_id"]
        conn.execute(
            "UPDATE ctl_pending SET approval_id = ? WHERE workflow_id = ?", [approval_id, wid]
        )
        log_decision(conn, now, ts, wid, "proposal_recovered", payload.get("dim"),
                     {"proposal_id": proposal_id, "approval_id": approval_id})

    try:
        approval = get_json(client, f"/approvals/{approval_id}")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
            log_decision(conn, now, ts, wid, "approval_missing", payload.get("dim"),
                         {"approval_id": approval_id})
        else:
            raise
        return True

    status = approval["status"]
    if status == "approved":
        response = client.post(f"/workflows/{wid}/apply", json={"approval_id": approval_id})
        if response.status_code == 200:
            version = int(response.json()["version"])
            apply_transition(conn, now, wid, kind, payload, version)
            conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
            log_decision(conn, now, ts, wid, "approval_applied", payload.get("dim"),
                         {"approval_id": approval_id, "kind": kind, "version": version})
            print(f"  applied {approval_id} [{kind}] version {version}")
        elif response.status_code in (409, 410):
            # Supervisor moved the config, or the apply window lapsed.
            conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
            update_state(conn, wid, retry_after_minute=now + RETRY_AFTER_REJECTION)
            log_decision(conn, now, ts, wid, "approval_apply_conflict", payload.get("dim"),
                         {"approval_id": approval_id, "status": response.status_code,
                          "body": response.text[:500]})
            print(f"  apply conflict {approval_id}: {response.status_code}")
        else:
            log_decision(conn, now, ts, wid, "approval_apply_failed", payload.get("dim"),
                         {"approval_id": approval_id, "status": response.status_code,
                          "body": response.text[:500]})
    elif status == "applied":
        # We applied before a crash but did not persist the transition.
        version = int(get_json(client, f"/workflows/{wid}/config")["version"])
        apply_transition(conn, now, wid, kind, payload, version)
        conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
        log_decision(conn, now, ts, wid, "approval_reconciled", payload.get("dim"),
                     {"approval_id": approval_id, "kind": kind, "version": version})
    elif status in ("rejected", "expired", "lapsed"):
        conn.execute("DELETE FROM ctl_pending WHERE workflow_id = ?", [wid])
        update_state(conn, wid, retry_after_minute=now + RETRY_AFTER_REJECTION)
        log_decision(conn, now, ts, wid, f"approval_{status}", payload.get("dim"),
                     {"approval_id": approval_id, "reasons": approval.get("reasons", [])})
        print(f"  approval {status} {approval_id}: {approval.get('reasons', [])}")
    return True


# --------------------------------------------------------------------------- decisions
def start_experiment(conn, client, now, ts, workflow, config, version, state):
    wid = workflow["workflow_id"]
    options = workflow.get("options") or {}
    dims = experiment_dims(workflow)
    if not dims:
        return
    dim = dims[int(state["dim_index"]) % len(dims)]

    current = distribution(config, dim)
    if current:
        # Deterministic tie-break so a restart picks the same control.
        control = max(sorted(current), key=lambda value: current[value])
    else:
        control = options[dim][0]

    # Uniform exploration gives every option an equal chance.  We keep the
    # control in the mix, so the comparison is like-for-like in time.
    weights = {value: 1.0 for value in options[dim]}
    new_config = set_dim_allocation(config, dim, weights)

    max_requests = int(workflow["governance"].get("max_requests_per_24h", 4))
    if approvals_last_24h(client, wid, now) >= max_requests:
        update_state(conn, wid, retry_after_minute=now + 2 * 60)
        log_decision(conn, now, ts, wid, "proposal_deferred", dim,
                     {"reason": "max_requests_per_24h", "limit": max_requests})
        return

    if same_dim_allocation(config, new_config, dim):
        # The config is already a uniform split (e.g. a dimension with two
        # options at 50/50).  No governance call is needed to start measuring.
        update_state(
            conn, wid,
            phase="exploring", dim=dim, control_value=control,
            champion_value=None, applied_version=version, started_minute=now,
            promoted_minute=None, last_check_minute=now, retry_after_minute=None,
            dim_index=int(state["dim_index"]) + 1,
        )
        log_decision(conn, now, ts, wid, "experiment_started", dim,
                     {"control": control, "options": options[dim], "already_uniform": True})
        print(f"  experiment started [{wid}/{dim}] control={control} (already uniform)")
        return

    primary, guardrail_metric, _ = primary_and_guardrail(workflow)
    summary = f"Explore {dim} for {wid} across {', '.join(options[dim])}"
    rationale = (
        f"Uniform allocation on {dim} to estimate {primary} against control {control}. "
        f"Winner requires a two-proportion z-test with guardrail on {guardrail_metric}; "
        f"no change is promoted from a single call."
    )
    submit_change(
        conn, client, now, ts, workflow, config, new_config, version, dim, "explore",
        {"dim": dim, "control": control}, summary, rationale,
    )


def evaluate_experiment(conn, client, now, ts, workflow, config, version, state):
    wid = workflow["workflow_id"]
    dim = state["dim"]
    control = state["control_value"]
    since = int(state["started_minute"])
    primary, guardrail_metric, guardrail_max_drop = primary_and_guardrail(workflow)

    # Time-based window only: a caller-ID health rotation may have bumped the
    # config version mid-experiment, but that change is orthogonal to this
    # dimension's variant assignment.
    stats = variant_stats(conn, wid, dim, since, 0)
    c_s, c_n, c_rate = metric_parts(stats.get(control), primary)

    if now - since >= MAX_EXPERIMENT_MINUTES:
        if approvals_last_24h(client, wid, now) >= int(workflow["governance"].get("max_requests_per_24h", 4)):
            update_state(conn, wid, retry_after_minute=now + 2 * 60)
            return
        new_config = set_dim_allocation(config, dim, {control: 1.0})
        if same_dim_allocation(config, new_config, dim):
            # Already back on the control: just close the experiment.
            update_state(conn, wid, phase="idle", dim=None, champion_value=None,
                         applied_version=version, retry_after_minute=None)
            log_decision(conn, now, ts, wid, "experiment_abandoned", dim,
                         {"control": control, "already_control": True})
            return
        summary = f"Stop {dim} exploration for {wid}: no significant winner"
        rationale = (
            f"No challenger beat control {control} on {primary} within "
            f"{MAX_EXPERIMENT_MINUTES // 1440} days at the configured sample sizes. "
            f"Returning the dimension to the control rather than promoting noise."
        )
        if submit_change(conn, client, now, ts, workflow, config, new_config, version, dim,
                         "abandon", {"dim": dim, "control": control}, summary, rationale):
            log_decision(conn, now, ts, wid, "experiment_abandoned", dim,
                         {"control": control, "control_rate": round(c_rate, 4)})
        return

    challengers = [value for value in workflow["options"][dim] if value != control]
    threshold = z_threshold(len(challengers))
    guard_min_n = MIN_DIALS_PER_ARM if guardrail_metric == "pickup_rate" else MIN_CONNECTED_PER_ARM
    eligible = []
    for value in challengers:
        v_s, v_n, v_rate = metric_parts(stats.get(value), primary)
        min_n = MIN_CONNECTED_PER_ARM if primary == "engagement_rate" else MIN_DIALS_PER_ARM
        if c_n < min_n or v_n < min_n:
            continue
        z = two_proportion_z(c_s, c_n, v_s, v_n)
        g_c_s, g_c_n, g_c_rate = metric_parts(stats.get(control), guardrail_metric)
        g_v_s, g_v_n, g_v_rate = metric_parts(stats.get(value), guardrail_metric)
        guard_violation = (g_c_n >= guard_min_n and g_v_n >= guard_min_n
                           and (g_c_rate - g_v_rate) > guardrail_max_drop + 1e-9)
        if z >= threshold and v_rate > c_rate and not guard_violation:
            eligible.append((v_rate, z, value, v_n))

    if not eligible:
        return
    # Do not promote from the first few hours: traffic mix and time-of-day
    # differ, and the sample-size gate alone does not protect against that.
    if now - since < MIN_EXPERIMENT_MINUTES:
        return
    best_rate, best_z, champion, champion_n = max(eligible)
    holdback = {champion: 0.9, control: 0.1}
    new_config = set_dim_allocation(config, dim, holdback)
    if same_dim_allocation(config, new_config, dim):
        # The holdback allocation is already in place (e.g. we promoted just
        # before a crash), so record the promotion without a new proposal.
        log_decision(conn, now, ts, wid, "winner_found", dim,
                     {"champion": champion, "control": control, "z": round(best_z, 3),
                      "champion_rate": round(best_rate, 4), "control_rate": round(c_rate, 4),
                      "already_applied": True})
        update_state(conn, wid, phase="exploit", champion_value=champion,
                     promoted_minute=now, last_check_minute=now, retry_after_minute=None)
        return
    if approvals_last_24h(client, wid, now) >= int(workflow["governance"].get("max_requests_per_24h", 4)):
        update_state(conn, wid, retry_after_minute=now + 2 * 60)
        return

    summary = f"Promote {dim}={champion} for {wid} ({primary} +{(best_rate - c_rate) * 100:.1f}pp)"
    rationale = (
        f"{champion} beat control {control} on {primary} with z={best_z:.2f} "
        f"(threshold {threshold:.2f}, n={champion_n}). Guardrail {guardrail_metric} "
        f"was within {guardrail_max_drop:.2f}. Retaining a 10% control holdback so decay can be detected."
    )
    if submit_change(conn, client, now, ts, workflow, config, new_config, version, dim,
                     "promote", {"dim": dim, "control": control, "champion": champion,
                                 "primary": primary}, summary, rationale):
        log_decision(conn, now, ts, wid, "winner_found", dim,
                     {"champion": champion, "control": control, "z": round(best_z, 3),
                      "champion_rate": round(best_rate, 4), "control_rate": round(c_rate, 4)})


def evaluate_exploit(conn, client, now, ts, workflow, config, version, state):
    wid = workflow["workflow_id"]
    if now - int(state["last_check_minute"] or 0) < DECAY_CHECK_MINUTES:
        return
    update_state(conn, wid, last_check_minute=now)

    dim = state["dim"]
    champion = state["champion_value"]
    control = state["control_value"]
    if not dim or not champion or not control or champion == control:
        return

    primary, guardrail_metric, guardrail_max_drop = primary_and_guardrail(workflow)
    since = max(int(state["promoted_minute"] or 0), now - 2 * 24 * 60)
    stats = variant_stats(conn, wid, dim, since, 0)
    c_s, c_n, c_rate = metric_parts(stats.get(control), primary)
    h_s, h_n, h_rate = metric_parts(stats.get(champion), primary)
    min_n = MIN_CONNECTED_PER_ARM if primary == "engagement_rate" else MIN_DIALS_PER_ARM
    if c_n < min_n or h_n < min_n:
        return

    z = two_proportion_z(c_s, c_n, h_s, h_n)
    g_c_s, g_c_n, g_c_rate = metric_parts(stats.get(control), guardrail_metric)
    g_h_s, g_h_n, g_h_rate = metric_parts(stats.get(champion), guardrail_metric)
    guard_min_n = MIN_DIALS_PER_ARM if guardrail_metric == "pickup_rate" else MIN_CONNECTED_PER_ARM
    guard_violation = (g_c_n >= guard_min_n and g_h_n >= guard_min_n
                       and (g_c_rate - g_h_rate) > guardrail_max_drop + 1e-9)

    if z > -DECAY_Z and not guard_violation:
        return
    if approvals_last_24h(client, wid, now) >= int(workflow["governance"].get("max_requests_per_24h", 4)):
        update_state(conn, wid, retry_after_minute=now + 2 * 60)
        return

    new_config = set_dim_allocation(config, dim, {control: 1.0})
    if same_dim_allocation(config, new_config, dim):
        # Already on the control (e.g. we rolled back before a crash).
        update_state(conn, wid, phase="idle", dim=None, champion_value=None,
                     applied_version=version, retry_after_minute=None)
        log_decision(conn, now, ts, wid, "decay_detected", dim,
                     {"champion": champion, "control": control, "already_control": True})
        return
    summary = f"Roll back {dim}={champion} for {wid}: decay detected"
    rationale = (
        f"Champion {champion} is now worse than control {control} on {primary} "
        f"(z={z:.2f}, champion {h_rate * 100:.1f}% vs control {c_rate * 100:.1f}%, "
        f"n={h_n}) or breached guardrail {guardrail_metric}. Rolling back."
    )
    if submit_change(conn, client, now, ts, workflow, config, new_config, version, dim,
                     "rollback", {"dim": dim, "control": control, "champion": champion,
                                  "primary": primary}, summary, rationale):
        log_decision(conn, now, ts, wid, "decay_detected", dim,
                     {"champion": champion, "control": control, "z": round(z, 3),
                      "guardrail_violation": guard_violation})


def decide_workflow(conn, client, now, ts, workflow):
    wid = workflow["workflow_id"]
    ensure_state(conn, wid)

    # 1. Reconcile any outstanding approval before doing anything new.
    if handle_pending(conn, client, now, ts, wid):
        return

    state = get_state(conn, wid)
    config_response = get_json(client, f"/workflows/{wid}/config")
    config = config_response["config"]
    version = int(config_response["version"])

    # 2. A version jump we did not make means a supervisor edited the config.
    #    Never silently overwrite it: abandon the current experiment and restart
    #    from the new baseline.
    applied = state["applied_version"]
    if state["phase"] in ("exploring", "exploit") and applied is not None and version > int(applied):
        log_decision(conn, now, ts, wid, "external_config_change", state["dim"],
                     {"from_version": int(applied), "to_version": version,
                      "action": "reset experiment"})
        update_state(conn, wid, phase="idle", dim=None, champion_value=None,
                     applied_version=version, retry_after_minute=None)
        state = get_state(conn, wid)

    retry_after = state["retry_after_minute"]
    if retry_after is not None and now < int(retry_after):
        return

    # 3. Caller-ID health is an incident path, so it takes priority over the
    #    next normal experiment.  It is skipped while a caller_id experiment is
    #    already measuring that dimension.
    if check_caller_health(conn, client, now, ts, workflow, config, version, state):
        return

    if state["phase"] == "idle":
        start_experiment(conn, client, now, ts, workflow, config, version, state)
    elif state["phase"] == "exploring":
        evaluate_experiment(conn, client, now, ts, workflow, config, version, state)
    elif state["phase"] == "exploit":
        evaluate_exploit(conn, client, now, ts, workflow, config, version, state)


def decide_all(conn, client, now, ts, workflows):
    for workflow in workflows.values():
        try:
            decide_workflow(conn, client, now, ts, workflow)
        except Exception as exc:  # one workflow must not take down the loop
            print(f"  decision error [{workflow['workflow_id']}]: {type(exc).__name__}: {exc}")


# --------------------------------------------------------------------------- main loop
def run():
    global D0
    conn = connect()
    client = httpx.Client(base_url=SIMULATOR_URL, timeout=120.0)
    state = get_json(client, "/sim/state")

    # Derive D0 from the API itself instead of assuming a date.  The sim gives
    # both the wall clock and the simulated minute, so this works for any run.
    D0 = datetime.fromisoformat(state["ts"]) - timedelta(minutes=int(state["sim_minute"]))

    if reset_if_new_run(conn, state):
        print("New simulator world detected: cleared controller state.")

    workflows = {w["workflow_id"]: w for w in get_json(client, "/workflows")}
    for wid in workflows:
        ensure_state(conn, wid)

    print(f"Scenario {state['scenario']} from {state['ts']} "
          f"({len(workflows)} workflows, credits {state['credits_remaining']})")
    bootstrap(conn, client, state)

    now = int(state["sim_minute"])
    ts = state["ts"]
    tick = int(state["tick_minutes"])
    step = max(tick, (ADVANCE_MINUTES // tick) * tick)
    finished = bool(state["finished"])

    while not finished:
        decide_all(conn, client, now, ts, workflows)

        response = client.post("/sim/advance", json={"minutes": step})
        if response.status_code == 409:
            break
        response.raise_for_status()
        state = response.json()
        for chunk in state.get("new_chunks", []):
            ingest_chunk(conn, client, chunk)
        now = int(state["sim_minute"])
        ts = state["ts"]
        finished = bool(state["finished"])
        meta_set(conn, "last_seen_minute", now)

    # One last look at the final tick's data.
    decide_all(conn, client, now, ts, workflows)

    final = get_json(client, "/sim/state")
    calls = conn.execute("SELECT COUNT(*), COUNT(DISTINCT lead_id) FROM ctl_calls").fetchone()
    print(f"Finished {final['scenario']} at {final['ts']}: "
          f"{calls[0]} calls stored for {calls[1]} leads, "
          f"{final['credits_remaining']} credits remaining.")
    client.close()
    conn.close()


def audit(limit=50):
    conn = duckdb.connect(DB_FILE, read_only=True)
    rows = conn.execute(
        "SELECT ts, workflow_id, action, dim, detail FROM ctl_decisions "
        "ORDER BY sim_minute DESC LIMIT ?", [limit]
    ).fetchall()
    for ts, wid, action, dim, detail in rows:
        print(f"{ts}  {wid:16}  {action:24}  {dim or '-':16}  {detail}")


if __name__ == "__main__":
    if "--audit" in sys.argv:
        audit()
    else:
        run()
