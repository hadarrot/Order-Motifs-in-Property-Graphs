#!/usr/bin/env python3
# Example:
#   python3 experiments_UD.py --password ItayBachar88 --setup_timeout 20 --timeout 10 --edges $(seq 20 20 300)
#   python3 experiments_UD.py --password ItayBachar88 --setup_timeout 20 --timeout 10 --edges $(seq 20 20 300) --debug 10
import argparse
import csv
import json
import sys
import time
import multiprocessing
from typing import Optional, Tuple, List
from pathlib import Path
from collections import Counter

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, AuthError

# ----------------------------
# Cypher (single-statement each)
# ----------------------------
RESET = "MATCH (n) DETACH DELETE n"

SCHEMA_STMTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS "
    "FOR (a:Account) REQUIRE a.id IS UNIQUE",

    "CREATE INDEX stageud_acc_level_phase IF NOT EXISTS "
    "FOR (s:StageUD) ON (s.accId, s.level, s.phase)",
]

POPULATE_ACCOUNTS = """
UNWIND range(1, $n_accounts) AS i
CREATE (:Account {id: i})
"""

POPULATE_TRANSFERS = """
MATCH (a:Account)
WITH collect(a) AS nodes, size(collect(a)) AS n
UNWIND range(1, $n_edges) AS k
WITH nodes, n,
     toInteger(rand()*n) AS i,
     toInteger(rand()*n) AS j
WITH nodes[i] AS u, nodes[j] AS v
WHERE u <> v
CREATE (u)-[:TRANSFER {
  amount: toInteger(1 + rand()*1000),
  ts: datetime("2025-01-01") + duration({days: toInteger(rand()*365)})
}]->(v)
"""

# -------- UD Lifted graph build (StageLift-UD) --------
# Build StageUD nodes: for each Account v, levels = {-1} ∪ incoming amounts; phases = U, Peak, D
BUILD_STAGEUD_NODES = """
MATCH (v:Account)
OPTIONAL MATCH (:Account)-[e:TRANSFER]->(v)
WITH v, collect(DISTINCT e.amount) AS inAmts
WITH v, [-1] + [amt IN inAmts WHERE amt IS NOT NULL] AS levels
UNWIND levels AS level
FOREACH (ph IN ['U','Peak','D'] |
  MERGE (:StageUD {accId: v.id, level: level, phase: ph})
)
"""

# U -> U  (strictly increasing)
BUILD_STAGEUD_EDGES_UU = """
MATCH (u:Account)-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
MATCH (su:StageUD {accId: u.id, phase:'U'})
WHERE su.level < j
MERGE (sv:StageUD {accId: v.id, level: j, phase:'U'})
MERGE (su)-[le:TRANSFER_LIFT_UD {amount: j}]->(sv)
  ON CREATE SET le.ts = ts, le.baseRelEid = elementId(e)
"""

# U -> Peak  (choose peak immediately after an increasing step)
BUILD_STAGEUD_EDGES_UP = """
MATCH (u:Account)-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
MATCH (su:StageUD {accId: u.id, phase:'U'})
WHERE su.level < j
MERGE (sv:StageUD {accId: v.id, level: j, phase:'Peak'})
MERGE (su)-[le:TRANSFER_LIFT_UD {amount: j}]->(sv)
  ON CREATE SET le.ts = ts, le.baseRelEid = elementId(e)
"""

# Peak -> D  (first strictly decreasing step)
BUILD_STAGEUD_EDGES_PD = """
MATCH (u:Account)-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
MATCH (su:StageUD {accId: u.id, phase:'Peak'})
WHERE j < su.level
MERGE (sv:StageUD {accId: v.id, level: j, phase:'D'})
MERGE (su)-[le:TRANSFER_LIFT_UD {amount: j}]->(sv)
  ON CREATE SET le.ts = ts, le.baseRelEid = elementId(e)
"""

# D -> D  (continue strictly decreasing)
BUILD_STAGEUD_EDGES_DD = """
MATCH (u:Account)-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
MATCH (su:StageUD {accId: u.id, phase:'D'})
WHERE j < su.level
MERGE (sv:StageUD {accId: v.id, level: j, phase:'D'})
MERGE (su)-[le:TRANSFER_LIFT_UD {amount: j}]->(sv)
  ON CREATE SET le.ts = ts, le.baseRelEid = elementId(e)
"""

def build_ud(session, build_timeout: Optional[float] = None):
    run_write(session, BUILD_STAGEUD_NODES, timeout_sec=build_timeout)
    run_write(session, BUILD_STAGEUD_EDGES_UU, timeout_sec=build_timeout)
    run_write(session, BUILD_STAGEUD_EDGES_UP, timeout_sec=build_timeout)
    run_write(session, BUILD_STAGEUD_EDGES_PD, timeout_sec=build_timeout)
    run_write(session, BUILD_STAGEUD_EDGES_DD, timeout_sec=build_timeout)

# -------- Baseline & lifted counts (min 2 edges; count all peak choices) --------
BASELINE_UD_COUNT = """
CALL () {
  MATCH p = (s:Account)-[:TRANSFER*]->(t:Account)
  WITH [r IN relationships(p) | r.amount] AS a
  WHERE size(a) >= 2
  UNWIND range(0, size(a)-2) AS k
  WITH a, k
  WHERE
    ALL(i IN range(1, k) WHERE a[i-1] < a[i]) AND
    ALL(i IN range(k+1, size(a)-1) WHERE a[i-1] > a[i])
  RETURN 1 AS row
}
RETURN count(*) AS rows
"""

# Note: prevent reusing the same base relationship along a lifted path
LIFTED_UD_COUNT = """
CALL () {
  MATCH (start:StageUD {level: -1, phase:'U'})
  MATCH p = (start)-[:TRANSFER_LIFT_UD*]->(x:StageUD {phase:'D'})
  WHERE length(p) >= 2
  WITH relationships(p) AS rs
  WHERE ALL(r IN rs WHERE r.baseRelEid IS NOT NULL)
    AND ALL(i IN range(0, size(rs)-1)
            WHERE NOT rs[i].baseRelEid IN [r IN rs[i+1..] | r.baseRelEid])
  RETURN 1 AS row
}
RETURN count(*) AS rows
"""

# -------- Debug signature extraction (multiset) --------
# Signature format: "r1-r2-...-rm|k", where r* are raw relationship elementIds; k is edge index of the peak.
DEBUG_BASELINE_SIGS = """
MATCH p = (s:Account)-[:TRANSFER*]->(t:Account)
WITH p,
     [r IN relationships(p) | r.amount] AS a,
     [r IN relationships(p) | elementId(r)] AS rid
WHERE size(a) >= 2
UNWIND range(0, size(a)-2) AS k
WITH rid, a, k
WHERE ALL(i IN range(1, k) WHERE a[i-1] < a[i])
  AND ALL(i IN range(k+1, size(a)-1) WHERE a[i-1] > a[i])
WITH reduce(s = "", x IN rid |
     s + CASE s WHEN "" THEN "" ELSE "-" END + x) AS rid_str, k
RETURN rid_str + "|" + toString(k) AS sig
"""

DEBUG_LIFTED_SIGS = """
MATCH (start:StageUD {level:-1, phase:'U'})
MATCH p = (start)-[:TRANSFER_LIFT_UD*]->(x:StageUD {phase:'D'})
WHERE length(p) >= 2
WITH p, nodes(p) AS ns, relationships(p) AS rs
WHERE ALL(r IN rs WHERE r.baseRelEid IS NOT NULL)
  AND ALL(i IN range(0, size(rs)-1)
          WHERE NOT rs[i].baseRelEid IN [r IN rs[i+1..] | r.baseRelEid])
WITH ns, rs,
     [i IN range(0, size(ns)-1) WHERE ns[i].phase = 'Peak'][0] AS peak_node_idx,
     reduce(s = "", r IN rs |
       s + CASE s WHEN "" THEN "" ELSE "-" END + r.baseRelEid) AS rid_str
WITH rid_str, (peak_node_idx - 1) AS k_edge
RETURN rid_str + "|" + toString(k_edge) AS sig
"""

# ----------------------------
# Helpers
# ----------------------------
TIMEOUT_CODES = {
    "Neo.ClientError.Transaction.TransactionTimedOut",
    "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
}

def is_timeout_error(err: Neo4jError) -> bool:
    code = getattr(err, "code", "") or ""
    msg = str(err).lower()
    return (code in TIMEOUT_CODES) or ("timed out" in msg)

def run_write(session, cypher: str, params=None, timeout_sec: Optional[float] = None):
    """Run a SINGLE Cypher statement in write mode."""
    def work(tx):
        tx.run(cypher, params or {}, timeout=timeout_sec).consume()
    session.execute_write(work)

def run_query_worker(pipe_conn, uri, user, password, database, cypher, params):
    """Child process: connect to Neo4j, run query, send (rows, elapsed_sec) back."""
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session(database=database) as s:
            t0 = time.perf_counter()
            rec = s.run(cypher, params or {}).single()
            elapsed = time.perf_counter() - t0
            rows = 0 if rec is None else (rec["rows"] if "rows" in rec else list(rec.values())[0])
        pipe_conn.send(("ok", (int(rows), float(elapsed))))
    except Exception as e:
        pipe_conn.send(("error", str(e)))
    finally:
        pipe_conn.close()

def run_count_with_latency(
    uri: str, user: str, password: str, database: str,
    cypher: str, params=None, timeout_sec: Optional[float] = None
) -> Tuple[int, float]:
    """Run a read query in a separate process so we can kill it if it hangs."""
    parent_conn, child_conn = multiprocessing.Pipe()
    p = multiprocessing.Process(
        target=run_query_worker,
        args=(child_conn, uri, user, password, database, cypher, params)
    )
    p.start()
    if parent_conn.poll(timeout_sec):
        status, data = parent_conn.recv()
        p.join()
        if status == "ok":
            return data  # (rows, elapsed)
        else:
            raise Neo4jError("ClientError", data)
    else:
        p.terminate()
        p.join()
        raise Neo4jError("ClientEnforcedTimeout", f"Query exceeded {timeout_sec}s")

def run_list(session, cypher: str, params=None, timeout_sec: Optional[float] = None) -> List[str]:
    """Run a read query and return a list of 'sig' strings."""
    res = []
    def work(tx):
        nonlocal res
        cur = tx.run(cypher, params or {}, timeout=timeout_sec)
        res = [r["sig"] for r in cur]
    session.execute_read(work)
    return res

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="UD (Up-then-Down) motif: baseline vs lifted build time, query latencies, timeouts, and sanity checks.")
    ap.add_argument("--uri", default="bolt://127.0.0.1:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", default="neo4j")
    ap.add_argument("--accounts", type=int, default=100)
    ap.add_argument("--edges", type=int, nargs="+",
                    default=[100, 110, 120, 130, 140, 150])
    ap.add_argument("--repeats", type=int, default=10,
                    help="Number of baseline/lifted runs per edge count")
    ap.add_argument("--setup_timeout", type=float, default=15.0,
                    help="Timeout (seconds) for RESET/SCHEMA/POPULATE")
    ap.add_argument("--build_timeout", type=float, default=60.0,
                    help="Timeout (seconds) for building the lifted graph")
    ap.add_argument("--timeout", type=float, default=5.0,
                    help="Timeout (seconds) for each baseline run")
    ap.add_argument("--lift_timeout", type=float, default=None,
                    help="Timeout (seconds) for each lifted run (defaults to --timeout)")
    ap.add_argument("--out", default="ud_baseline_lifted_timeouts.csv")
    ap.add_argument("--debug", type=int, default=0,
                    help="If >0 and counts differ, print up to N example mismatches from each side.")
    ap.add_argument("--debug_timeout", type=float, default=20.0,
                    help="Timeout (seconds) for each debug signature query.")
    args = ap.parse_args()
    if args.lift_timeout is None:
        args.lift_timeout = args.timeout

    # Timestamped output filename
    ts = time.strftime("%Y%m%d-%H%M%S")
    out_path = args.out
    if "{ts}" in out_path:
        out_path = out_path.format(ts=ts)
    else:
        p = Path(out_path)
        out_path = str(p.with_name(f"{p.stem}_{ts}{p.suffix or '.csv'}"))

    # Connect + auth sanity check
    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        with driver.session(database=args.database) as s:
            run_write(s, "RETURN 1", timeout_sec=5.0)
    except AuthError:
        print("Authentication failed: check --user/--password.", file=sys.stderr)
        sys.exit(1)

    # COMPACT rows only
    compact_rows: List[dict] = []

    with driver.session(database=args.database) as s:
        for E in args.edges:
            print(f"Building base graph for E={E} ...", flush=True)

            # Fresh graph for this edge count (use setup timeout)
            run_write(s, RESET, timeout_sec=args.setup_timeout)
            for stmt in SCHEMA_STMTS:
                run_write(s, stmt, timeout_sec=args.setup_timeout)

            # --- Time the ORIGINAL graph build (populate accounts + transfers) ---
            print("  Populating base graph ...", flush=True)
            t0 = time.perf_counter()
            run_write(
                s, POPULATE_ACCOUNTS,
                params={"n_accounts": args.accounts},
                timeout_sec=args.setup_timeout
            )
            run_write(
                s, POPULATE_TRANSFERS,
                params={"n_edges": E},
                timeout_sec=args.setup_timeout
            )
            baseline_build_ms = round((time.perf_counter() - t0) * 1000.0, 3)
            print(f"  Baseline build time: {baseline_build_ms} ms", flush=True)

            # Build lifted UD graph once per E, and time it (nodes + edges)
            print("  Building lifted UD graph (StageUD) ...", flush=True)
            t0 = time.perf_counter()
            build_ud(s, build_timeout=args.build_timeout)
            lifted_build_ms = round((time.perf_counter() - t0) * 1000.0, 3)
            print(f"  Lifted UD build time:  {lifted_build_ms} ms", flush=True)

            # Per-run results
            base_timeouts = 0
            base_successes = 0
            base_lat_ms: List[object] = []
            base_counts: List[object] = []
            base_status: List[str] = []

            lift_timeouts = 0
            lift_successes = 0
            lift_lat_ms: List[object] = []
            lift_counts: List[object] = []
            lift_status: List[str] = []

            sanity_equal_counts: List[Optional[bool]] = []  # per-run True/False/None

            for _ in range(args.repeats):
                # Baseline UD
                b_rows: Optional[int] = None
                try:
                    rows, elapsed = run_count_with_latency(
                        args.uri, args.user, args.password, args.database,
                        BASELINE_UD_COUNT, timeout_sec=args.timeout
                    )
                    base_successes += 1
                    base_lat_ms.append(round(elapsed * 1000.0, 3))
                    base_counts.append(int(rows))
                    base_status.append("ok")
                    b_rows = int(rows)
                except Neo4jError as e:
                    if is_timeout_error(e) or getattr(e, "args", [None])[0] == "ClientEnforcedTimeout":
                        base_timeouts += 1
                        base_lat_ms.append("")
                        base_counts.append("")
                        base_status.append("timeout")
                    else:
                        raise

                # Lifted UD
                l_rows: Optional[int] = None
                try:
                    rows, elapsed = run_count_with_latency(
                        args.uri, args.user, args.password, args.database,
                        LIFTED_UD_COUNT, timeout_sec=args.lift_timeout
                    )
                    lift_successes += 1
                    lift_lat_ms.append(round(elapsed * 1000.0, 3))
                    lift_counts.append(int(rows))
                    lift_status.append("ok")
                    l_rows = int(rows)
                except Neo4jError as e:
                    if is_timeout_error(e) or getattr(e, "args", [None])[0] == "ClientEnforcedTimeout":
                        lift_timeouts += 1
                        lift_lat_ms.append("")
                        lift_counts.append("")
                        lift_status.append("timeout")
                    else:
                        raise

                # Sanity per run: equal counts if both succeeded
                if (b_rows is not None) and (l_rows is not None):
                    sanity_equal_counts.append(bool(b_rows == l_rows))
                else:
                    sanity_equal_counts.append(None)

            # Averages (successful runs only)
            base_ok_lat = [x for x in base_lat_ms if isinstance(x, (int, float))]
            base_avg_ms = round(sum(base_ok_lat) / len(base_ok_lat), 3) if base_ok_lat else ""

            lift_ok_lat = [x for x in lift_lat_ms if isinstance(x, (int, float))]
            lift_avg_ms = round(sum(lift_ok_lat) / len(lift_ok_lat), 3) if lift_ok_lat else ""

            sanity_ok_runs = sum(1 for x in sanity_equal_counts if x is True)
            sanity_mismatch_runs = sum(1 for x in sanity_equal_counts if x is False)

            # Choose a representative count (they should all be equal when sanity_ok)
            count_sample = next((c for c in base_counts if isinstance(c, int)), "")
            speedup = round(base_avg_ms / lift_avg_ms, 3) if (isinstance(base_avg_ms, float) and isinstance(lift_avg_ms, float) and lift_avg_ms > 0) else ""

            # Append COMPACT row
            compact_rows.append({
                "edges": E,
                "runs": args.repeats,
                "count_per_run": count_sample,
                "baseline_avg_ms": base_avg_ms,
                "lifted_avg_ms": lift_avg_ms,
                "speedup_x": speedup,
                "baseline_timeouts": base_timeouts,
                "lifted_timeouts": lift_timeouts,
                "baseline_build_ms": baseline_build_ms,
                "lifted_build_ms": lifted_build_ms,
                "sanity_ok_runs": sanity_ok_runs,
                "sanity_mismatch_runs": sanity_mismatch_runs,
            })

            # Console summary
            print(
                f"E={E}: base BUILD={baseline_build_ms} ms; "
                f"baseline avg={base_avg_ms} ms ({base_successes}/{args.repeats} ok, {base_timeouts} timeouts); "
                f"lifted UD BUILD={lifted_build_ms} ms; lifted avg={lift_avg_ms} ms "
                f"({lift_successes}/{args.repeats} ok, {lift_timeouts} timeouts); "
                f"speedup≈{speedup}×; sanity ok runs={sanity_ok_runs}, mismatches={sanity_mismatch_runs}",
                flush=True
            )

            # ---------- DEBUG: print mismatches ----------
            if args.debug > 0 and sanity_mismatch_runs > 0:
                print(f"[DEBUG] Counts differ for E={E}. Collecting signatures ...", flush=True)
                try:
                    baseline_sigs = run_list(s, DEBUG_BASELINE_SIGS, timeout_sec=args.debug_timeout)
                    lifted_sigs   = run_list(s, DEBUG_LIFTED_SIGS,   timeout_sec=args.debug_timeout)
                    cb = Counter(baseline_sigs)
                    cl = Counter(lifted_sigs)
                    only_b = list((cb - cl).elements())
                    only_l = list((cl - cb).elements())
                    print(f"[DEBUG] baseline total sigs: {sum(cb.values())}, lifted total sigs: {sum(cl.values())}")
                    print(f"[DEBUG] in baseline only: {len(only_b)}, in lifted only: {len(only_l)}")
                    if only_b:
                        print("[DEBUG] sample baseline-only:")
                        for sig in only_b[:args.debug]:
                            print("  ", sig)
                    if only_l:
                        print("[DEBUG] sample lifted-only:")
                        for sig in only_l[:args.debug]:
                            print("  ", sig)
                except Neo4jError as e:
                    print(f"[DEBUG] Error while collecting debug signatures: {e}", file=sys.stderr)

    # ----- write COMPACT CSV -----
    fieldnames = [
        "edges", "runs",
        "count_per_run",
        "baseline_avg_ms", "lifted_avg_ms", "speedup_x",
        "baseline_timeouts", "lifted_timeouts",
        "baseline_build_ms", "lifted_build_ms",
        "sanity_ok_runs", "sanity_mismatch_runs",
    ]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(compact_rows)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass
    main()
