#!/usr/bin/env python3
"""
experiments_gmark_unbounded_rebuild.py

- Base graph is generated with gMark.
- For each run: generate fresh graph, load, build StageLift, run queries.
- Writes TWO output files incrementally:
  1. *_details.csv: Full details with JSON arrays. "timeout" used for timeouts.
  2. *_summary.csv: Clean summary. "N/A" used for missing averages.
"""

import argparse
import csv
import json
import sys
import time
import multiprocessing
import os
import subprocess
import glob
import random
from pathlib import Path
from typing import Optional, Tuple

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, AuthError

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
except ImportError:
    print("tqdm not found. Install with `pip install tqdm` for progress bars.")
    def tqdm(iterable, desc=None, leave=True):
        return iterable

# ----------------------------
# Cypher fragments
# ----------------------------

RESET = """
MATCH (n)
DETACH DELETE n
"""

SCHEMA_STMTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level)",
]

POPULATE_ACCOUNTS = """
UNWIND $ids AS i
MERGE (:Account {id: i})
"""

POPULATE_EDGES = """
UNWIND $rows AS r
MATCH (u:Account {id: r.src})
MATCH (v:Account {id: r.dst})
CREATE (u)-[:TRANSFER {
  amount: toInteger(r.amount),
  ts: datetime("2025-01-01") + duration({days: toInteger(rand()*365)})
}]->(v)
"""

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

BASELINE_COUNT = """
CALL (){
  MATCH p = (s:Account)-[:TRANSFER*]->(t:Account)
  WITH s, p, [r IN relationships(p) | r.amount] AS amts, t
  WHERE size(amts) > 0
    AND ALL(i IN range(1, size(amts)-1) WHERE amts[i-1] < amts[i])
  RETURN 1 AS row
}
RETURN count(*) AS rows
"""

LIFTED_COUNT = """
CALL (){
  MATCH (start:Stage {level: -1})
  MATCH p = (start)-[:TRANSFER_LIFT*]->(x:Stage)
  RETURN 1 AS row
}
RETURN count(*) AS rows
"""

# ----------------------------
# gMark helpers
# ----------------------------

def run_cmd(cmd, cwd=None):
    result = subprocess.run(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result

def update_transfers_xml(schema_path: str, n_edges: int, n_nodes: int) -> None:
    edges_per_node_avg = n_edges / n_nodes if n_nodes > 0 else 1.0
    edges_per_node = max(1, int(round(edges_per_node_avg))) if edges_per_node_avg >= 0.5 else 1

    xml_content = f'''<generator>
  <graph><nodes>0</nodes></graph>
  <predicates><size>{n_edges}</size><alias symbol="0">transfer</alias></predicates>
  <types><size>1</size><alias type="0">Account</alias></types>
  <schema>
    <source type="0">
      <target type="0" symbol="0" multiplicity="*">
        <indistribution type="uniform"><min>{edges_per_node}</min><max>{edges_per_node}</max></indistribution>
        <outdistribution type="uniform"><min>{edges_per_node}</min><max>{edges_per_node}</max></outdistribution>
      </target>
    </source>
  </schema>
  <workload id="0" size="0"/>
</generator>'''
    with open(schema_path, "w") as f:
        f.write(xml_content)

def generate_graph_gmark(schema: str, n_nodes: int, n_edges: int, gmark_dir: str, output_dir: str) -> str:
    src_dir = os.path.join(gmark_dir, "src")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_edges = os.path.join(output_dir, "edges.csv")

    schema_path = os.path.join(gmark_dir, f"use-cases/{schema}")
    update_transfers_xml(schema_path, n_edges, n_nodes)

    cmd = ["./test", "-c", f"../use-cases/{schema}", "-g", "../demo/play/play-graph.txt", "-w", "../demo/play/play-workload.xml", "-r", "../demo/play", "-n", str(n_nodes)]
    run_cmd(cmd, cwd=src_dir)

    graph_pattern = os.path.join(src_dir, "../demo/play/play-graph.txt*")
    graph_files = sorted(glob.glob(graph_pattern))
    if not graph_files:
        raise RuntimeError(f"No graph files found at {graph_pattern}")

    with open(csv_edges, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["src", "dst", "amount"])
        for gf in graph_files:
            with open(gf, "r") as fin:
                for line in fin:
                    parts = line.strip().split()
                    if len(parts) == 3:
                        try:
                            src = int(parts[0])
                            dst = int(parts[2])
                        except ValueError:
                            continue
                        amount = random.randint(1, 1000)
                        writer.writerow([src, dst, amount])
    return csv_edges

def load_graph_from_csv(session, csv_path: str, setup_timeout: float) -> int:
    edges = []
    nodes = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u, v, amt = int(row["src"]), int(row["dst"]), int(row["amount"])
            edges.append({"src": u, "dst": v, "amount": amt})
            nodes.add(u)
            nodes.add(v)
    
    node_ids = sorted(nodes)
    run_write(session, POPULATE_ACCOUNTS, params={"ids": node_ids}, timeout_sec=setup_timeout)
    
    BATCH = 50000
    for i in range(0, len(edges), BATCH):
        run_write(session, POPULATE_EDGES, params={"rows": edges[i:i+BATCH]}, timeout_sec=setup_timeout)
    return len(edges)

# ----------------------------
# Neo4j helpers
# ----------------------------

TIMEOUT_CODES = {"Neo.ClientError.Transaction.TransactionTimedOut", "Neo.TransientError.Database.DatabaseUnavailable"}

def is_timeout_error(err: Exception) -> bool:
    code = getattr(err, "code", "") or ""
    return (code in TIMEOUT_CODES) or ("timed out" in str(err).lower())

def run_write(session, cypher: str, params=None, timeout_sec=None):
    def work(tx):
        tx.run(cypher, params or {}, timeout=timeout_sec).consume()
    session.execute_write(work)

def run_query_worker(pipe_conn, uri, user, password, database, cypher, params):
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session(database=database) as s:
            t0 = time.perf_counter()
            rec = s.run(cypher, params or {}).single()
            elapsed = time.perf_counter() - t0
            rows = rec["rows"] if (rec and "rows" in rec) else (list(rec.values())[0] if rec else 0)
        pipe_conn.send(("ok", (int(rows), float(elapsed))))
    except Exception as e:
        pipe_conn.send(("error", str(e)))
    finally:
        pipe_conn.close()

def run_count_with_latency(uri, user, password, database, cypher, params=None, timeout_sec=None) -> Tuple[int, float]:
    parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
    p = multiprocessing.Process(target=run_query_worker, args=(child_conn, uri, user, password, database, cypher, params))
    p.daemon = True
    p.start()
    p.join(timeout=timeout_sec)
    if p.is_alive():
        p.terminate()
        p.join()
        raise Neo4jError("ClientEnforcedTimeout", f"Query exceeded {timeout_sec}s")
    
    if parent_conn.poll():
        status, payload = parent_conn.recv()
        if status == "ok": return payload
        else: raise Neo4jError("ClientError", payload)
    else:
        raise Neo4jError("ClientError", "No response from worker process")

# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Baseline vs Lifted (gMark rebuilds)")
    ap.add_argument("--uri", default="bolt://127.0.0.1:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", default="neo4j")
    ap.add_argument("--accounts", type=int, nargs="+", default=[100])
    ap.add_argument("--densities", type=float, nargs="+", default=[1.0])
    ap.add_argument("--repeats", type=int, default=10)
    ap.add_argument("--setup_timeout", type=float, default=20.0)
    ap.add_argument("--build_timeout", type=float, default=60.0)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--lift_timeout", type=float, default=None)
    ap.add_argument("--gmark_dir", default="gmark")
    ap.add_argument("--gmark_schema", default="shop.xml")
    ap.add_argument("--tmp_dir", default="/tmp")
    ap.add_argument("--out", default="experiment_results.csv")

    args = ap.parse_args()
    if args.lift_timeout is None: args.lift_timeout = args.timeout

    # Timestamp output
    ts = time.strftime("%Y%m%d-%H%M%S")
    p = Path(args.out)
    base_name = f"{p.stem}_{ts}"
    
    # Define TWO output files
    out_details = str(p.with_name(f"{base_name}_details.csv"))
    out_summary = str(p.with_name(f"{base_name}_summary.csv"))

    # Connect check
    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        with driver.session(database=args.database) as s:
            run_write(s, "RETURN 1", timeout_sec=5.0)
    except AuthError:
        print("Authentication failed.", file=sys.stderr)
        sys.exit(1)

    # ---------------------------------------------------------
    # Initialize Headers for BOTH files
    # ---------------------------------------------------------
    
    details_fields = [
        "nodes", "density", "edges", "runs",
        "baseline_build_ms", "lifted_build_ms",
        "baseline_build_runs_ms", "lifted_build_runs_ms",
        "baseline_timeouts", "baseline_successes", "baseline_avg_latency_ms",
        "baseline_run_latencies_ms", "baseline_run_counts", "baseline_run_statuses",
        "lift_timeouts", "lift_successes", "lift_avg_latency_ms",
        "lift_run_latencies_ms", "lift_run_counts", "lift_run_statuses",
        "sanity_equal_true", "sanity_equal_false", "sanity_equal_values",
    ]
    with open(out_details, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=details_fields)
        writer.writeheader()
        f.flush()

    summary_fields = [
        "Nodes", "Edges", 
        "Baseline Build (ms)", "Lifted Build (ms)", 
        "Baseline Avg Latency (ms)", "Lifted Avg Latency (ms)", 
        "Speedup"
    ]
    with open(out_summary, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        f.flush()

    print(f"Writing detailed results to: {out_details}", flush=True)
    print(f"Writing summary results to:  {out_summary}", flush=True)

    with driver:
        for N in args.accounts:
            for D in args.densities:
                E = int(N * D)
                
                # Containers for this config
                baseline_build_runs = []
                lifted_build_runs = []
                base_lat_ms = []
                base_counts = []
                base_status = []
                base_timeouts = 0
                base_successes = 0
                lift_lat_ms = []
                lift_counts = []
                lift_status = []
                lift_timeouts = 0
                lift_successes = 0
                sanity_equal_counts = []

                desc_str = f"N={N}, D={D} (E={E})"
                for r in tqdm(range(args.repeats), desc=desc_str):
                    output_dir = os.path.join(args.tmp_dir, f"gmark_N{N}_E{E}_run{r}")
                    
                    # 1. Gen Graph
                    csv_edges = generate_graph_gmark(args.gmark_schema, N, E, args.gmark_dir, output_dir)

                    with driver.session(database=args.database) as s:
                        # 2. Reset/Load
                        run_write(s, RESET, timeout_sec=args.setup_timeout)
                        for stmt in SCHEMA_STMTS: run_write(s, stmt, timeout_sec=args.setup_timeout)
                        
                        t0 = time.perf_counter()
                        load_graph_from_csv(s, csv_edges, setup_timeout=args.setup_timeout)
                        baseline_build_runs.append(round((time.perf_counter() - t0) * 1000.0, 3))

                        # 3. Build Lifted
                        t0 = time.perf_counter()
                        run_write(s, BUILD_STAGE_NODES, timeout_sec=args.build_timeout)
                        run_write(s, BUILD_STAGE_EDGES, timeout_sec=args.build_timeout)
                        lifted_build_runs.append(round((time.perf_counter() - t0) * 1000.0, 3))

                        # 4. Baseline Query
                        b_rows = None
                        try:
                            rows, elapsed = run_count_with_latency(args.uri, args.user, args.password, args.database, BASELINE_COUNT, timeout_sec=args.timeout)
                            base_successes += 1
                            base_lat_ms.append(round(elapsed * 1000.0, 3))
                            base_counts.append(int(rows))
                            base_status.append("ok")
                            b_rows = int(rows)
                        except Neo4jError as e:
                            if is_timeout_error(e) or getattr(e, "args", [None])[0] == "ClientEnforcedTimeout":
                                base_timeouts += 1
                                base_lat_ms.append("timeout")  # <-- Changed from "" to "timeout"
                                base_counts.append("timeout")
                                base_status.append("timeout")
                            else: raise

                        # 5. Lifted Query
                        l_rows = None
                        try:
                            rows, elapsed = run_count_with_latency(args.uri, args.user, args.password, args.database, LIFTED_COUNT, timeout_sec=args.lift_timeout)
                            lift_successes += 1
                            lift_lat_ms.append(round(elapsed * 1000.0, 3))
                            lift_counts.append(int(rows))
                            lift_status.append("ok")
                            l_rows = int(rows)
                        except Neo4jError as e:
                            if is_timeout_error(e) or getattr(e, "args", [None])[0] == "ClientEnforcedTimeout":
                                lift_timeouts += 1
                                lift_lat_ms.append("timeout")  # <-- Changed from "" to "timeout"
                                lift_counts.append("timeout")
                                lift_status.append("timeout")
                            else: raise

                        # 6. Sanity
                        if (b_rows is not None) and (l_rows is not None):
                            sanity_equal_counts.append(bool(b_rows == l_rows))
                        else:
                            sanity_equal_counts.append(None)

                # --- Aggregation ---
                # 1. Builds
                baseline_build_ms = round(sum(baseline_build_runs) / len(baseline_build_runs), 3) if baseline_build_runs else 0
                lifted_build_ms = round(sum(lifted_build_runs) / len(lifted_build_runs), 3) if lifted_build_runs else 0

                # 2. Baseline Avg (Numerical for calc, String for display)
                base_ok_lat = [x for x in base_lat_ms if isinstance(x, (int, float))]
                if base_ok_lat:
                    base_avg_num = sum(base_ok_lat) / len(base_ok_lat)
                    base_avg_details = round(base_avg_num, 3)
                    base_avg_summary = round(base_avg_num, 3)
                else:
                    base_avg_num = None
                    base_avg_details = "timeout"   # Details: "timeout"
                    base_avg_summary = "N/A"  # Summary: "N/A"

                # 3. Lifted Avg
                lift_ok_lat = [x for x in lift_lat_ms if isinstance(x, (int, float))]
                if lift_ok_lat:
                    lift_avg_num = sum(lift_ok_lat) / len(lift_ok_lat)
                    lift_avg_details = round(lift_avg_num, 3)
                    lift_avg_summary = round(lift_avg_num, 3)
                else:
                    lift_avg_num = None
                    lift_avg_details = "timeout"
                    lift_avg_summary = "N/A"

                # --- Calculate Speedup ---
                # Speedup = (Baseline Avg Query) / (Lifted Build + Lifted Avg Query)
                speedup = "N/A"
                if (base_avg_num is not None) and (lift_avg_num is not None):
                    total_lifted_cost = lifted_build_ms + lift_avg_num
                    if total_lifted_cost > 0:
                        speedup = round(base_avg_num / total_lifted_cost, 3)

                # --- Write Details Row ---
                detail_row = {
                    "nodes": N, "density": D, "edges": E, "runs": args.repeats,
                    "baseline_build_ms": baseline_build_ms,
                    "lifted_build_ms": lifted_build_ms,
                    "baseline_build_runs_ms": json.dumps(baseline_build_runs),
                    "lifted_build_runs_ms": json.dumps(lifted_build_runs),
                    "baseline_timeouts": base_timeouts,
                    "baseline_successes": base_successes,
                    "baseline_avg_latency_ms": base_avg_details, # Uses "timeout" if null
                    "baseline_run_latencies_ms": json.dumps(base_lat_ms),
                    "baseline_run_counts": json.dumps(base_counts),
                    "baseline_run_statuses": json.dumps(base_status),
                    "lift_timeouts": lift_timeouts,
                    "lift_successes": lift_successes,
                    "lift_avg_latency_ms": lift_avg_details,    # Uses "timeout" if null
                    "lift_run_latencies_ms": json.dumps(lift_lat_ms),
                    "lift_run_counts": json.dumps(lift_counts),
                    "lift_run_statuses": json.dumps(lift_status),
                    "sanity_equal_true": sum(1 for x in sanity_equal_counts if x is True),
                    "sanity_equal_false": sum(1 for x in sanity_equal_counts if x is False),
                    "sanity_equal_values": json.dumps(sanity_equal_counts),
                }

                with open(out_details, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=details_fields)
                    writer.writerow(detail_row)
                    f.flush()
                    os.fsync(f.fileno())

                # --- Write Summary Row ---
                summary_row = {
                    "Nodes": N,
                    "Edges": E,
                    "Baseline Build (ms)": baseline_build_ms,
                    "Lifted Build (ms)": lifted_build_ms,
                    "Baseline Avg Latency (ms)": base_avg_summary, # Uses "N/A" if null
                    "Lifted Avg Latency (ms)": lift_avg_summary,   # Uses "N/A" if null
                    "Speedup": speedup
                }

                with open(out_summary, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=summary_fields)
                    writer.writerow(summary_row)
                    f.flush()
                    os.fsync(f.fileno())

                print(f"  > Saved N={N}, E={E} (Speedup={speedup})")

    print(f"\nExperiment complete.")
    print(f"Details: {out_details}")
    print(f"Summary: {out_summary}")

if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass
    main()
