#!/usr/bin/env python3
"""
experiments_gmark_unbounded_rebuild.py

Same logic as Experiments.py (baseline vs lifted, unbounded strictly-increasing
TRANSFER paths), but:

- Base graph is generated with gMark.
- For *each run* (repeat) and *each E*, we:
    * generate a fresh graph with gMark,
    * RESET + load into Neo4j,
    * build the StageLift graph,
    * run baseline + lifted queries once.

Thus every run sees a different random graph, and counts can differ between runs.
"""

# python3 experiments_gmark_unbounded.py     --password ItayBachar88     --accounts 1000 5000 25000     --densities 1.0 5.0 10.0         --out my_experiment_results.csv
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

# ----------------------------
# Cypher fragments (schema + StageLift + queries)
# ----------------------------

RESET = """
MATCH (n)
DETACH DELETE n
"""

SCHEMA_STMTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level)",
]

# For loading from CSV (gMark)
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

# StageLift build
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

# Baseline / lifted queries (as in Experiments.py)
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
    """Run a shell command and raise on error, printing stdout/stderr."""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def update_transfers_xml(schema_path: str, n_edges: int, n_nodes: int) -> None:
    """
    Write a very simple gMark schema that generates Account->Account edges with a
    controllable out-degree (edges per node).
    """
    edges_per_node_avg = n_edges / n_nodes if n_nodes > 0 else 1.0
    if edges_per_node_avg < 0.5:
        edges_per_node = 1
    else:
        edges_per_node = max(1, int(round(edges_per_node_avg)))

    xml_content = f'''<generator>

  <graph>
    <!-- allow -n on the CLI to set node count -->
    <nodes>0</nodes>
  </graph>

  <predicates>
    <size>{n_edges}</size>
    <alias symbol="0">transfer</alias>
  </predicates>

  <types>
    <size>1</size>
    <alias type="0">Account</alias>
    <!-- do not fix type sizes here so overall -n controls total nodes -->
  </types>

  <schema>
    <!-- Account -> Account edges directly (no Transfer nodes). -->
    <source type="0">
      <target type="0" symbol="0" multiplicity="*">
        <indistribution type="uniform">
          <min>{edges_per_node}</min>
          <max>{edges_per_node}</max>
        </indistribution>
        <outdistribution type="uniform">
          <min>{edges_per_node}</min>
          <max>{edges_per_node}</max>
        </outdistribution>
      </target>
    </source>
  </schema>

  <workload id="0" size="0"/>

</generator>'''
    with open(schema_path, "w") as f:
        f.write(xml_content)


def generate_graph_gmark(schema: str, n_nodes: int, n_edges: int,
                         gmark_dir: str, output_dir: str) -> str:
    """
    Run gMark to generate a graph, then merge all resulting edges into
    a single CSV edges file with columns: src, dst, amount.

    We let gMark control the structural distribution; we just pick a random
    amount in [1,1000] for each edge.
    """
    src_dir = os.path.join(gmark_dir, "src")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_edges = os.path.join(output_dir, "edges.csv")

    # Update schema XML with target edge count and rough out-degree
    schema_path = os.path.join(gmark_dir, f"use-cases/{schema}")
    update_transfers_xml(schema_path, n_edges, n_nodes)

    # gMark expects relative paths from src/
    graph_base_rel = "../demo/play/play-graph.txt"
    workload_rel   = "../demo/play/play-workload.xml"
    html_rel       = "../demo/play"

    # Run gMark generator
    cmd = [
        "./test",
        "-c", f"../use-cases/{schema}",
        "-g", graph_base_rel,
        "-w", workload_rel,
        "-r", html_rel,
        "-n", str(n_nodes),
    ]
    run_cmd(cmd, cwd=src_dir)

    # Collect generated graph files
    graph_pattern = os.path.join(src_dir, "../demo/play/play-graph.txt*")
    graph_files = sorted(glob.glob(graph_pattern))
    if not graph_files:
        raise RuntimeError(f"No graph files found at {graph_pattern}")

    # Merge into edges.csv
    with open(csv_edges, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["src", "dst", "amount"])
        for gf in graph_files:
            with open(gf, "r") as fin:
                for line in fin:
                    parts = line.strip().split()
                    if len(parts) != 3:
                        continue
                    # gMark outputs: subject predicate object
                    try:
                        src = int(parts[0])
                    except ValueError:
                        src = parts[0]
                    try:
                        dst = int(parts[2])
                    except ValueError:
                        dst = parts[2]
                    amount = random.randint(1, 1000)
                    writer.writerow([src, dst, amount])

    return csv_edges


def load_graph_from_csv(session, csv_path: str, setup_timeout: float) -> int:
    """
    Load the gMark-generated CSV into Neo4j.
    Returns the number of edges loaded.
    """
    edges = []
    nodes = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = int(row["src"])
            v = int(row["dst"])
            amt = int(row["amount"])
            edges.append({"src": u, "dst": v, "amount": amt})
            nodes.add(u)
            nodes.add(v)

    node_ids = sorted(nodes)

    # Create Account nodes
    run_write(
        session,
        POPULATE_ACCOUNTS,
        params={"ids": node_ids},
        timeout_sec=setup_timeout,
    )

    # Batch CREATE edges
    BATCH = 50000
    for i in range(0, len(edges), BATCH):
        chunk = edges[i:i+BATCH]
        run_write(
            session,
            POPULATE_EDGES,
            params={"rows": chunk},
            timeout_sec=setup_timeout,
        )

    return len(edges)

# ----------------------------
# Neo4j helpers (similar to Experiments.py)
# ----------------------------

TIMEOUT_CODES = {
    "Neo.ClientError.Transaction.TransactionTimedOut",
    "Neo.TransientError.Database.DatabaseUnavailable",
}

def is_timeout_error(err: Exception) -> bool:
    code = getattr(err, "code", "") or ""
    msg = str(err).lower()
    return (code in TIMEOUT_CODES) or ("timed out" in msg)


def run_write(session, cypher: str, params=None, timeout_sec: Optional[float] = None) -> None:
    """Run a single write statement with an optional timeout."""
    def work(tx):
        tx.run(cypher, params or {}, timeout=timeout_sec).consume()
    session.execute_write(work)


def run_query_worker(pipe_conn,
                     uri: str, user: str, password: str, database: str,
                     cypher: str, params):
    """Child process: connect, run query, send (rows, elapsed_sec) back."""
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session(database=database) as s:
            t0 = time.perf_counter()
            rec = s.run(cypher, params or {}).single()
            elapsed = time.perf_counter() - t0
            if rec is None:
                rows = 0
            else:
                # Prefer "rows" column if present, else the first value
                if "rows" in rec:
                    rows = rec["rows"]
                else:
                    rows = list(rec.values())[0]
        pipe_conn.send(("ok", (int(rows), float(elapsed))))
    except Exception as e:
        pipe_conn.send(("error", str(e)))
    finally:
        pipe_conn.close()


def run_count_with_latency(
    uri: str, user: str, password: str, database: str,
    cypher: str, params=None, timeout_sec: Optional[float] = None
) -> Tuple[int, float]:
    """
    Run a read-only query in a separate process, enforcing a wall-clock timeout.
    Returns (row_count, elapsed_seconds) or raises Neo4jError("ClientEnforcedTimeout").
    """
    parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
    p = multiprocessing.Process(
        target=run_query_worker,
        args=(child_conn, uri, user, password, database, cypher, params),
    )
    p.daemon = True
    p.start()

    p.join(timeout=timeout_sec)
    if p.is_alive():
        p.terminate()
        p.join()
        raise Neo4jError("ClientEnforcedTimeout", f"Query exceeded {timeout_sec}s")

    if parent_conn.poll():
        status, payload = parent_conn.recv()
        if status == "ok":
            return payload
        else:
            raise Neo4jError("ClientError", payload)
    else:
        raise Neo4jError("ClientError", "No response from worker process")

# ----------------------------
# Main experiment loop (gMark + rebuild per run)
# ----------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Baseline vs Lifted (unbounded) with gMark-generated graphs, rebuilding per run."
    )
    ap.add_argument("--uri", default="bolt://127.0.0.1:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", default="neo4j")
 
    ap.add_argument("--accounts", type=int, nargs="+", default=[100],
                    help="Target number of accounts (nodes) for gMark. Can specify multiple values.")
    ap.add_argument("--densities", type=float, nargs="+",
                    default=[0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
                    help="Graph densities to test. Edges = Accounts * Density. Can specify multiple values.")
    ap.add_argument("--repeats", type=int, default=10,
                    help="Number of runs per edge count. EACH run rebuilds the graph.")

    ap.add_argument("--setup_timeout", type=float, default=20.0,
                    help="Timeout (seconds) for RESET/SCHEMA/LOAD.")
    ap.add_argument("--build_timeout", type=float, default=60.0,
                    help="Timeout (seconds) for building the lifted graph.")
    ap.add_argument("--timeout", type=float, default=5.0,
                    help="Timeout (seconds) for each baseline run.")
    ap.add_argument("--lift_timeout", type=float, default=None,
                    help="Timeout (seconds) for each lifted run (defaults to --timeout).")

    # gMark-specific flags
    ap.add_argument("--gmark_dir", default="gmark",
                    help="Path to gMark checkout (dir containing src/ and use-cases/).")
    ap.add_argument("--gmark_schema", default="shop.xml",
                    help="Schema XML file under gmark/use-cases/ to overwrite & use.")
    ap.add_argument("--tmp_dir", default="/tmp",
                    help="Where to emit per-run edges.csv files.")

    ap.add_argument("--out", default="baseline_lifted_gmark_rebuild.csv")

    args = ap.parse_args()
    if args.lift_timeout is None:
        args.lift_timeout = args.timeout

    # Timestamp output
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

    results = []

    with driver:
        for N in args.accounts:
            for D in args.densities:
                E = int(N * D)
                print(f"=== N={N}, D={D} (E={E}) ===", flush=True)

                baseline_build_runs = []
                lifted_build_runs = []

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

                sanity_equal_counts = []

                for r in range(args.repeats):
                    print(f"  run {r+1}/{args.repeats}", flush=True)

                    output_dir = os.path.join(args.tmp_dir, f"gmark_N{N}_E{E}_run{r}")
                    csv_edges = generate_graph_gmark(
                        schema=args.gmark_schema,
                        n_nodes=N,
                        n_edges=E,
                        gmark_dir=args.gmark_dir,
                        output_dir=output_dir,
                    )

                    with driver.session(database=args.database) as s:
                        # Reset graph + (re)apply schema
                        run_write(s, RESET, timeout_sec=args.setup_timeout)
                        for stmt in SCHEMA_STMTS:
                            run_write(s, stmt, timeout_sec=args.setup_timeout)

                        # Load graph (baseline build for this run)
                        t0 = time.perf_counter()
                        loaded_edges = load_graph_from_csv(
                            s, csv_edges, setup_timeout=args.setup_timeout
                        )
                        baseline_build_ms_run = round(
                            (time.perf_counter() - t0) * 1000.0, 3
                        )
                        baseline_build_runs.append(baseline_build_ms_run)
                        print(f"    run {r+1}: loaded {loaded_edges} edges in {baseline_build_ms_run} ms",
                              flush=True)

                        # Build StageLift for this run
                        t0 = time.perf_counter()
                        run_write(s, BUILD_STAGE_NODES, timeout_sec=args.build_timeout)
                        run_write(s, BUILD_STAGE_EDGES, timeout_sec=args.build_timeout)
                        lifted_build_ms_run = round(
                            (time.perf_counter() - t0) * 1000.0, 3
                        )
                        lifted_build_runs.append(lifted_build_ms_run)
                        print(f"    run {r+1}: lifted build {lifted_build_ms_run} ms", flush=True)

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

                        # Print baseline query latency/status for this run
                        if base_status:
                            last_status = base_status[-1]
                            if last_status == "ok":
                                print(f"    baseline query: {base_counts[-1]} rows in {base_lat_ms[-1]} ms", flush=True)
                            else:
                                print(f"    baseline query: {last_status}", flush=True)

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

                        # Print lifted query latency/status for this run
                        if lift_status:
                            last_status = lift_status[-1]
                            if last_status == "ok":
                                print(f"    lifted query: {lift_counts[-1]} rows in {lift_lat_ms[-1]} ms", flush=True)
                            else:
                                print(f"    lifted query: {last_status}", flush=True)

                        # Per-run sanity
                        if (b_rows is not None) and (l_rows is not None):
                            sanity_equal_counts.append(bool(b_rows == l_rows))
                        else:
                            sanity_equal_counts.append(None)

            # Aggregate per-E
            baseline_build_ms = (
                round(sum(baseline_build_runs) / len(baseline_build_runs), 3)
                if baseline_build_runs else ""
            )
            lifted_build_ms = (
                round(sum(lifted_build_runs) / len(lifted_build_runs), 3)
                if lifted_build_runs else ""
            )

            base_ok_lat = [x for x in base_lat_ms if isinstance(x, (int, float))]
            base_avg_ms = round(sum(base_ok_lat) / len(base_ok_lat), 3) if base_ok_lat else ""

            lift_ok_lat = [x for x in lift_lat_ms if isinstance(x, (int, float))]
            lift_avg_ms = round(sum(lift_ok_lat) / len(lift_ok_lat), 3) if lift_ok_lat else ""

            sanity_ok_runs = sum(1 for x in sanity_equal_counts if x is True)
            sanity_mismatch_runs = sum(1 for x in sanity_equal_counts if x is False)

            results.append({
                "nodes": N,
                "density": D,
                "edges": E,
                "runs": args.repeats,

                "baseline_build_ms": baseline_build_ms,
                "lifted_build_ms": lifted_build_ms,

                "baseline_timeouts": base_timeouts,
                "baseline_successes": base_successes,
                "baseline_avg_latency_ms": base_avg_ms,
                "baseline_run_latencies_ms": json.dumps(base_lat_ms),
                "baseline_run_counts": json.dumps(base_counts),
                "baseline_run_statuses": json.dumps(base_status),

                "lift_timeouts": lift_timeouts,
                "lift_successes": lift_successes,
                "lift_avg_latency_ms": lift_avg_ms,
                "lift_run_latencies_ms": json.dumps(lift_lat_ms),
                "lift_run_counts": json.dumps(lift_counts),
                "lift_run_statuses": json.dumps(lift_status),

                "sanity_equal_true": sanity_ok_runs,
                "sanity_equal_false": sanity_mismatch_runs,
                "sanity_equal_values": json.dumps(sanity_equal_counts),
            })

    # Write CSV
    fieldnames = [
        "nodes", "density", "edges", "runs",
        "baseline_build_ms", "lifted_build_ms",
        "baseline_timeouts", "baseline_successes", "baseline_avg_latency_ms",
        "baseline_run_latencies_ms", "baseline_run_counts", "baseline_run_statuses",
        "lift_timeouts", "lift_successes", "lift_avg_latency_ms",
        "lift_run_latencies_ms", "lift_run_counts", "lift_run_statuses",
        "sanity_equal_true", "sanity_equal_false", "sanity_equal_values",
    ]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    # Write summary section
    with open(out_path, "a", newline="") as f:
        f.write("\n\n# Summary\n")
        f.write("Nodes,Edges,Baseline Build (ms),Lifted Build (ms),Baseline Avg Latency (ms),Lifted Avg Latency (ms),Speedup\n")
        
        for result in results:
            edges = result["edges"]
            nodes = result["nodes"]
            baseline_build = result["baseline_build_ms"]
            lifted_build = result["lifted_build_ms"]
            baseline_latency = result["baseline_avg_latency_ms"]
            lifted_latency = result["lift_avg_latency_ms"]
            
            # Calculate speedup (baseline_latency / lifted_latency)
            speedup = ""
            if isinstance(baseline_latency, (int, float)) and isinstance(lifted_latency, (int, float)):
                if lifted_latency > 0:
                    speedup = round(baseline_latency / lifted_latency, 3)
            
            f.write(f"{nodes},{edges},{baseline_build},{lifted_build},{baseline_latency},{lifted_latency},{speedup}\n")
    
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass
    main()
