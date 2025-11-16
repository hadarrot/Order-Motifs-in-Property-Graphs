#!/usr/bin/env python3
"""
experiments_bounded_paths.py

flag guide:
  --uri              Bolt URI (default bolt://127.0.0.1:7687)
  --user             Neo4j username (default neo4j)
  --password         Neo4j password (REQUIRED)
  --database         Neo4j database name (default neo4j)
  --accounts         N: number of Account nodes to create per build
  --edges            E list: edge counts to test (supports shell seq)
  --hops             H list: max path length(s); each H means 1..H
  --repeats          Number of timing runs per (E,H)
  --setup_timeout    Timeout for RESET/SCHEMA/POPULATE (seconds)
  --build_timeout    Timeout for lifted-graph build (seconds)
  --timeout          Timeout for baseline query (seconds)
  --lift_timeout     Timeout for lifted query (defaults to --timeout)
  --out              Output CSV path (timestamp auto-appended unless {ts} in name)

  NOTE: The script now ALWAYS rebuilds (RESET+populate+lift) before EVERY run.
"""

# Example:
#   python3 experiments_bounded_paths.py --password ItayBachar88 --setup_timeout 20 --timeout 10 --edges $(seq 100 20 160) --hops $(seq 5 5 10)

import argparse
import csv
import json
import sys
import time
import multiprocessing
from typing import Optional, Tuple
from pathlib import Path
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, AuthError

# ----------------------------
# Cypher (single-statement each)
# ----------------------------
RESET = "MATCH (n) DETACH DELETE n"

# One statement per run (Bolt rule)
SCHEMA_STMTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS "
    "FOR (a:Account) REQUIRE a.id IS UNIQUE",

    "CREATE INDEX stage_acc_level IF NOT EXISTS "
    "FOR (s:Stage) ON (s.accId, s.level)",
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
     toInteger(rand()*n) As j
WITH nodes[i] AS u, nodes[j] AS v
WHERE u <> v
CREATE (u)-[:TRANSFER {
  amount: toInteger(1 + rand()*1000),
  ts: datetime("2025-01-01") + duration({days: toInteger(rand()*365)})
}]->(v)
"""

# -------- Lifted graph build (StageLift) --------
BUILD_STAGE_NODES = """
MATCH (v:Account)
OPTIONAL MATCH (:Account)-[e:TRANSFER]->(v)
WITH v, collect(DISTINCT e.amount) AS inAmts
WITH v, [-1] + [amt IN inAmts WHERE amt IS NOT NULL] AS levels
UNWIND levels AS level
MERGE (:Stage {accId: v.id, level: level})
"""

BUILD_STAGE_EDGES = """
MATCH (u:Account)-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
MATCH (su:Stage {accId: u.id})
WHERE su.level < j
MERGE (sv:Stage {accId: v.id, level: j})
MERGE (su)-[le:TRANSFER_LIFT {amount: j}]->(sv)
  ON CREATE SET le.ts = ts
"""

# -------- Bounded baseline & lifted counts (strictly increasing amounts) --------
def make_BASELINE_COUNT(max_hops: int) -> str:
    # Count strictly-increasing paths with 1..max_hops edges in the ORIGINAL graph.
    return f"""
CALL(){{
  MATCH p = (s:Account)-[:TRANSFER*1..{max_hops}]->(t:Account)
  WITH s, p, [r IN relationships(p) | r.amount] AS amts, t
  WHERE size(amts) > 0
    AND ALL(i IN range(1, size(amts)-1) WHERE amts[i-1] < amts[i])
  RETURN 1 AS row
}}
RETURN count(*) AS rows
"""

def make_LIFTED_COUNT(max_hops: int) -> str:
    # Count paths with 1..max_hops edges in the LIFTED graph (StageLift).
    return f"""
CALL(){{
  MATCH (start:Stage {{level: -1}})
  MATCH p = (start)-[:TRANSFER_LIFT*1..{max_hops}]->(x:Stage)
  RETURN 1 AS row
}}
RETURN count(*) AS rows
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

def build_graph_once(session, accounts: int, edges: int,
                     setup_timeout: float, build_timeout: float) -> Tuple[float, float]:
    """RESET + SCHEMA + populate + build lifted. Return (baseline_build_ms, lifted_build_ms)."""
    # Fresh graph
    run_write(session, RESET, timeout_sec=setup_timeout)
    for stmt in SCHEMA_STMTS:
        run_write(session, stmt, timeout_sec=setup_timeout)

    # Populate base graph
    t0 = time.perf_counter()
    run_write(session, POPULATE_ACCOUNTS, params={"n_accounts": accounts}, timeout_sec=setup_timeout)
    run_write(session, POPULATE_TRANSFERS, params={"n_edges": edges}, timeout_sec=setup_timeout)
    baseline_build_ms = round((time.perf_counter() - t0) * 1000.0, 3)

    # Build lifted graph
    t0 = time.perf_counter()
    run_write(session, BUILD_STAGE_NODES, timeout_sec=build_timeout)
    run_write(session, BUILD_STAGE_EDGES, timeout_sec=build_timeout)
    lifted_build_ms = round((time.perf_counter() - t0) * 1000.0, 3)

    return baseline_build_ms, lifted_build_ms

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Baseline vs Lifted with BOUNDED path length: sweep over edges and hops.")
    ap.add_argument("--uri", default="bolt://127.0.0.1:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", default="neo4j")
    ap.add_argument("--accounts", type=int, default=100)

    ap.add_argument("--edges", type=int, nargs="+",
                    default=[50, 75, 100, 125, 150, 175, 200, 225, 250, 300],
                    help="List of edge counts to build (supports shell seq).")
    ap.add_argument("--hops", type=int, nargs="+",
                    default=[2, 3, 4, 5],
                    help="List of max hops to test (supports shell seq). Each H bounds paths as 1..H.")

    ap.add_argument("--repeats", type=int, default=10,
                    help="Number of baseline/lifted runs per (edges,hops) pair")
    ap.add_argument("--setup_timeout", type=float, default=15.0,
                    help="Timeout (seconds) for RESET/SCHEMA/POPULATE")
    ap.add_argument("--build_timeout", type=float, default=60.0,
                    help="Timeout (seconds) for building the lifted graph")
    ap.add_argument("--timeout", type=float, default=5.0,
                    help="Timeout (seconds) for each baseline run")
    ap.add_argument("--lift_timeout", type=float, default=None,
                    help="Timeout (seconds) for each lifted run (defaults to --timeout)")
    ap.add_argument("--out", default="baseline_lifted_bounded.csv")
    args = ap.parse_args()

    args.rebuild_each_run = True

    if args.lift_timeout is None:
        args.lift_timeout = args.timeout

    # Timestamped output filename
    ts = time.strftime("%Y%m%d-%H%M%S")  # local time
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

    results = []
    with driver.session(database=args.database) as s:
        for E in args.edges:
            print(f"=== E={E} ===", flush=True)

            # If not rebuilding each run: build once per E here
            if not args.rebuild_each_run:
                print("  Populating base graph ...", flush=True)
                baseline_build_ms, lifted_build_ms = build_graph_once(
                    s, args.accounts, E, args.setup_timeout, args.build_timeout
                )
                print(f"  Baseline build time: {baseline_build_ms} ms", flush=True)
                print(f"  Lifted   build time: {lifted_build_ms} ms", flush=True)

            for H in args.hops:
                BASELINE_COUNT = make_BASELINE_COUNT(H)
                LIFTED_COUNT   = make_LIFTED_COUNT(H)

                # Per (E,H) accumulators
                base_timeouts = 0
                base_successes = 0
                base_lat_ms = []
                base_counts = []
                base_status = []

                lift_timeouts = 0
                lift_successes = 0
                lift_lat_ms = []
                lift_counts = []
                lift_status = []

                sanity_equal_counts = []  # per run True/False/None

                # For rebuild_each_run: collect per-run build times to average
                per_run_baseline_build_ms = []
                per_run_lifted_build_ms = []

                for r in range(args.repeats):
                    if args.rebuild_each_run:
                        b_ms, l_ms = build_graph_once(
                            s, args.accounts, E, args.setup_timeout, args.build_timeout
                        )
                        per_run_baseline_build_ms.append(b_ms)
                        per_run_lifted_build_ms.append(l_ms)

                    # Baseline query
                    b_rows: Optional[int] = None
                    try:
                        rows, elapsed = run_count_with_latency(
                            args.uri, args.user, args.password, args.database,
                            BASELINE_COUNT, timeout_sec=args.timeout
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

                    # Lifted query
                    l_rows: Optional[int] = None
                    try:
                        rows, elapsed = run_count_with_latency(
                            args.uri, args.user, args.password, args.database,
                            LIFTED_COUNT, timeout_sec=args.lift_timeout
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
                sanity_all_equal = 1 if (sanity_mismatch_runs == 0 and sanity_ok_runs > 0) else 0  

                # Final build times to report for this (E,H)
                if args.rebuild_each_run:
                    baseline_build_ms = round(sum(per_run_baseline_build_ms) / len(per_run_baseline_build_ms), 3) if per_run_baseline_build_ms else ""
                    lifted_build_ms   = round(sum(per_run_lifted_build_ms)   / len(per_run_lifted_build_ms), 3) if per_run_lifted_build_ms else ""

                results.append({
                    "edges": E,
                    "hops": H,
                    "runs": args.repeats,

                    "baseline_build_ms": baseline_build_ms,
                    "lifted_build_ms": lifted_build_ms,

                    "baseline_timeouts": base_timeouts,
                    "baseline_successes": base_successes,
                    "baseline_avg_latency_ms": base_avg_ms,

                    "lifted_timeouts": lift_timeouts,
                    "lifted_successes": lift_successes,
                    "lifted_avg_latency_ms": lift_avg_ms,

                    "sanity_ok_runs": sanity_ok_runs,
                    "sanity_mismatch_runs": sanity_mismatch_runs,
                    "sanity_all_equal": sanity_all_equal,
                })

                print(
                    f"E={E}, H={H}: baseline avg={base_avg_ms} ms "
                    f"({base_successes}/{args.repeats} ok, {base_timeouts} to); "
                    f"lifted avg={lift_avg_ms} ms ({lift_successes}/{args.repeats} ok, {lift_timeouts} to); "
                    f"sanity ok={sanity_ok_runs}, mismatches={sanity_mismatch_runs}; "
                    f"builds (avg ms): base={baseline_build_ms}, lifted={lifted_build_ms}",
                    flush=True
                )

    # Write CSV
    fieldnames = [
        "edges", "hops", "runs",
        "baseline_build_ms", "lifted_build_ms",
        "baseline_timeouts", "baseline_successes", "baseline_avg_latency_ms",
        "lifted_timeouts", "lifted_successes", "lifted_avg_latency_ms",
        "sanity_ok_runs", "sanity_mismatch_runs", "sanity_all_equal",
    ]
    write_header = not Path(out_path).exists()
    with open(out_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(results)

    print(f"Wrote {out_path}")

if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass
    main()
