#!/usr/bin/env python3
"""
experiments_bounded_paths_gmark.py
Fully integrated version:
- Graph generation via Cypher OR gMark
- Loading edge CSV into Neo4j
- Running original bounded path experiments (baseline vs lifted)
"""

import argparse
import csv
import json
import sys
import time
import multiprocessing
import random
from typing import Optional, Tuple
from pathlib import Path
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, AuthError
import subprocess
import os
import glob


###############################################################################
# Helper: run shell commands
###############################################################################

def run_cmd(cmd, cwd=None):
    result = subprocess.run(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result.stdout


###############################################################################
# Helper: update transfers.xml with dynamic edge count
###############################################################################

def update_transfers_xml(schema_path, n_edges, n_nodes):
    """
    Update the transfers.xml schema file to generate the desired number of edges.
    
    For sparse graphs (edges_per_node < 0.5): Uses minimal schema to let Python generate exact edges
    For dense graphs (edges_per_node >= 0.5): Uses gMark's native distribution for efficient generation
    
    n_edges: target number of edges to generate
    n_nodes: total number of nodes
    """
    # Calculate edges per node
    edges_per_node_avg = n_edges / n_nodes if n_nodes > 0 else 1
    
    # For sparse graphs, use minimal edges_per_node (gMark will generate ~1 per node)
    # For denser graphs, use the calculated value
    if edges_per_node_avg < 0.5:
        edges_per_node = 1  # Minimal: gMark generates ~1 per node, Python selects n_edges
    else:
        edges_per_node = max(1, int(round(edges_per_node_avg)))
    
    # Write the transfers.xml schema with both distributions for better control
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
    <!-- Account -> Account edges directly (no Transfer nodes). Set multiplicity to control per-account outdegree -->
    <source type="0">
      <target type="0" symbol="0" multiplicity="*">
        <indistribution type="uniform">
          <min>{edges_per_node}</min>
          <max>{edges_per_node}</max>
        </indistribution>
        <outdistribution type="uniform">
          <!-- set min/max to desired per-account transfers -->
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


###############################################################################
# gMark graph generation + merge
###############################################################################

def generate_graph_gmark(schema, n_nodes, n_edges, gmark_dir, output_dir):
    """
    Generates a graph using gMark with n_nodes nodes and approximately n_edges edges.
    
    For dense graphs (edges_per_node >= 0.5): Uses gMark's native distribution to generate edges
    For sparse graphs (edges_per_node < 0.5): Uses gMark for node structure, then generates
                                               edges manually in Python to get the exact count
    """

    src_dir = os.path.join(gmark_dir, "src")
    
    # Calculate per-node edge count
    edges_per_node_avg = n_edges / n_nodes if n_nodes > 0 else 1
    
    # Update transfers.xml with the target edge count
    schema_path = os.path.join(gmark_dir, f"use-cases/{schema}")
    
    # Determine if we need to use Python fallback for sparse graphs
    use_python_fallback = edges_per_node_avg < 0.5
    update_transfers_xml(schema_path, n_edges, n_nodes)
    
    # use_python_fallback determined; proceed to generation

    # gMark expects relative paths from src/
    graph_base_rel = "../demo/play/play-graph.txt"
    workload_rel    = "../demo/play/play-workload.xml"
    html_rel        = "../demo/play"

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_edges = os.path.join(output_dir, "edges.csv")

    # Step 1: run gMark generator
    cmd1 = [
        "./test",
        "-c", f"../use-cases/{schema}",
        "-g", graph_base_rel,
        "-w", workload_rel,
        "-r", html_rel,
        "-n", str(n_nodes)
    ]
    run_cmd(cmd1, cwd=src_dir)

    # Step 2: collect all play-graph.txt* files
    graph_files_pattern = os.path.join(src_dir, "../demo/play/play-graph.txt*")
    graph_files = sorted(glob.glob(graph_files_pattern))

    if not graph_files:
        raise RuntimeError(f"No graph files found at {graph_files_pattern}")

    # Step 3: merge into edges.csv
    with open(csv_edges, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["src", "dst", "amount"])
        total = 0
        
        if use_python_fallback:
            # For sparse graphs, generate exactly n_edges with random connections
            nodes = list(range(n_nodes))
            
            for i in range(n_edges):
                src = random.choice(nodes)
                dst = random.choice(nodes)
                attempts = 0
                while src == dst and attempts < 10:
                    dst = random.choice(nodes)
                    attempts += 1
                if src != dst:
                    amount = random.randint(1, 1000)
                    writer.writerow([src, dst, amount])
                    total += 1
        else:
            # Use all gMark-generated edges
            for gf in graph_files:
                with open(gf, "r") as fin:
                    for line in fin:
                        parts = line.strip().split()
                        if len(parts) == 3:
                            # gMark outputs triples: subject predicate object
                            try:
                                src = int(parts[0])
                            except Exception:
                                src = parts[0]
                            try:
                                dst = int(parts[2])
                            except Exception:
                                dst = parts[2]
                            amount = random.randint(1, 1000)
                            writer.writerow([src, dst, amount])
                            total += 1

    return csv_edges


###############################################################################
# Cypher-based graph generator (your original)
###############################################################################

def generate_graph_cypher(accounts, edges):
    """
    Your original random graph generator stub.
    This is left as-is: returns a small CSV for now.
    """
    print(f"[Cypher-Gen] Using existing generator for edges={edges}")
    csv_path = f"/tmp/edges_{edges}.csv"
    with open(csv_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["src", "dst", "label"])
        writer.writerow(["1", "2", "10"])
        writer.writerow(["2", "3", "20"])
        writer.writerow(["3", "4", "30"])
    return csv_path


###############################################################################
# Cypher statements from your original script
###############################################################################

RESET = "MATCH (n) DETACH DELETE n"

SCHEMA_STMTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level)",
]

# Original StageLift build
POPULATE_ACCOUNTS = """
UNWIND $ids AS i
MERGE (:Account {id: i})
"""

POPULATE_EDGES = """
UNWIND $rows AS r
MATCH (u:Account {id: r.src})
MATCH (v:Account {id: r.dst})
CREATE (u)-[:TRANSFER {
  amount: toInteger(r.label),
  ts: datetime("2025-01-01") + duration({days: toInteger(rand()*365)})
}]->(v)
"""

# StageLift
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


###############################################################################
# Baseline and lifted bounded queries
###############################################################################

def make_BASELINE_COUNT(max_hops: int) -> str:
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
    return f"""
CALL(){{
  MATCH (start:Stage {{level: -1}})
  MATCH p = (start)-[:TRANSFER_LIFT*1..{max_hops}]->(x:Stage)
  RETURN 1 AS row
}}
RETURN count(*) AS rows
"""


###############################################################################
# Neo4j helpers (unchanged from your script)
###############################################################################

TIMEOUT_CODES = {
    "Neo.ClientError.Transaction.TransactionTimedOut",
    "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
}

def is_timeout_error(err: Neo4jError) -> bool:
    code = getattr(err, "code", "") or ""
    msg = str(err).lower()
    return (code in TIMEOUT_CODES) or ("timed out" in msg)

def run_write(session, cypher: str, params=None, timeout_sec: Optional[float] = None):
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
            return data
        else:
            raise Neo4jError("ClientError", data)
    else:
        p.terminate()
        p.join()
        raise Neo4jError("ClientEnforcedTimeout", f"Query exceeded {timeout_sec}s")


###############################################################################
# NEW: load graph from CSV instead of random generator
###############################################################################

def load_graph_from_csv(session, csv_path, setup_timeout):
    """
    Loads a graph from edges.csv into Neo4j:
    - Creates Account nodes for all distinct src/dst
    - Creates TRANSFER edges with amount = label
    """

    # Parse CSV
    edges = []
    nodes = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = int(row["src"])
            v = int(row["dst"])
            lbl = int(row["amount"])
            edges.append({"src": u, "dst": v, "label": lbl})
            nodes.add(u)
            nodes.add(v)

    node_list = sorted(nodes)

    # 1) Create Account nodes
    run_write(
        session,
        POPULATE_ACCOUNTS,
        params={"ids": node_list},
        timeout_sec=setup_timeout
    )

    # 2) Create TRANSFER edges
    # For performance: batch edges
    BATCH = 50000
    for i in range(0, len(edges), BATCH):
        chunk = edges[i:i + BATCH]
        run_write(
            session,
            POPULATE_EDGES,
            params={"rows": chunk},
            timeout_sec=setup_timeout
        )


###############################################################################
# (Unmodified) build_graph_once except source of graph data
###############################################################################

def build_graph_once(session, accounts: int, edges: int,
                     setup_timeout: float, build_timeout: float,
                     generator: str,
                     csv_edges: Optional[str] = None) -> Tuple[float, float]:
    """
    RESET → SCHEMA → populate base graph → build lifted.
    Modified so that if generator='gmark', population uses CSV instead of random edges.
    """

    run_write(session, RESET, timeout_sec=setup_timeout)
    for stmt in SCHEMA_STMTS:
        run_write(session, stmt, timeout_sec=setup_timeout)

    t0 = time.perf_counter()

    if generator == "gmark":
        assert csv_edges is not None
        load_graph_from_csv(session, csv_edges, setup_timeout)

    else:
        # original random generator
        run_write(session, POPULATE_ACCOUNTS, params={"ids": list(range(1, accounts + 1))}, timeout_sec=setup_timeout)
        run_write(session,
                  """
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
                  """,
                  params={"n_edges": edges},
                  timeout_sec=setup_timeout)

    baseline_build_ms = round((time.perf_counter() - t0)*1000, 3)

    # Build lifted
    t0 = time.perf_counter()
    run_write(session, BUILD_STAGE_NODES, timeout_sec=build_timeout)
    run_write(session, BUILD_STAGE_EDGES, timeout_sec=build_timeout)
    lifted_build_ms = round((time.perf_counter() - t0)*1000, 3)

    return baseline_build_ms, lifted_build_ms


###############################################################################
# MAIN EXECUTION LOOP (your original code, minimally modified)
###############################################################################

def main():
    ap = argparse.ArgumentParser(description="Baseline vs Lifted with bounded path length + gMark generation.")
    ap.add_argument("--uri", default="bolt://127.0.0.1:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", default="neo4j")
    ap.add_argument("--accounts", type=int, default=100)

    ap.add_argument("--edges", type=int, nargs="+", default=[100])
    ap.add_argument("--hops", type=int, nargs="+", default=[2,3,4])
    ap.add_argument("--repeats", type=int, default=10)

    ap.add_argument("--setup_timeout", type=float, default=20.0)
    ap.add_argument("--build_timeout", type=float, default=60.0)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--lift_timeout", type=float, default=None)

    ap.add_argument("--generator", choices=["cypher", "gmark"], default="cypher")
    ap.add_argument("--gmark_nodes", type=int, default=50000)
    ap.add_argument("--gmark_schema", default="shop.xml")
    ap.add_argument("--gmark_dir", default="gmark")

    ap.add_argument("--out", default="bounded_paths_gmark.csv")

    args = ap.parse_args()

    if args.lift_timeout is None:
        args.lift_timeout = args.timeout

    # Timestamp output
    ts = time.strftime("%Y%m%d-%H%M%S")
    out_path = f"{Path(args.out).stem}_{ts}.csv"

    # Connect & auth
    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        with driver.session(database=args.database) as s:
            run_write(s, "RETURN 1", timeout_sec=5)
    except AuthError:
        print("Authentication failed.")
        sys.exit(1)

    results = []

    for E in args.edges:

        with driver.session(database=args.database) as s:

            for H in args.hops:

                BASELINE_COUNT = make_BASELINE_COUNT(H)
                LIFTED_COUNT   = make_LIFTED_COUNT(H)

                per_run_base_build = []
                per_run_lift_build = []

                base_lat = []
                lift_lat = []

                base_timeouts = 0
                lift_timeouts = 0

                sanity_equal_counts = []

                for r in range(args.repeats):
                    
                    # Generate gMark graph for this run (inside the repeat loop)
                    if args.generator == "gmark":
                        output_dir = f"/tmp/gmark_graph_E{E}_run{r}"
                        Path(output_dir).mkdir(exist_ok=True, parents=True)
                        csv_edges = generate_graph_gmark(
                            schema=args.gmark_schema,
                            n_nodes=args.gmark_nodes,
                            n_edges=E,
                            gmark_dir=args.gmark_dir,
                            output_dir=output_dir
                        )
                    else:
                        csv_edges = None

                    b_ms, l_ms = build_graph_once(
                        s,
                        args.accounts,
                        E,
                        args.setup_timeout,
                        args.build_timeout,
                        generator=args.generator,
                        csv_edges=csv_edges
                    )
                    per_run_base_build.append(b_ms)
                    per_run_lift_build.append(l_ms)

                    # baseline
                    try:
                        rows, elapsed = run_count_with_latency(
                            args.uri, args.user, args.password, args.database,
                            BASELINE_COUNT, timeout_sec=args.timeout
                        )
                        base_lat.append(round(elapsed*1000, 3))
                        b_count = rows
                    except:
                        base_timeouts += 1
                        base_lat.append(None)
                        b_count = None

                    # lifted
                    try:
                        rows, elapsed = run_count_with_latency(
                            args.uri, args.user, args.password, args.database,
                            LIFTED_COUNT, timeout_sec=args.lift_timeout
                        )
                        lift_lat.append(round(elapsed*1000, 3))
                        l_count = rows
                    except:
                        lift_timeouts += 1
                        lift_lat.append(None)
                        l_count = None

                    if b_count is not None and l_count is not None:
                        sanity_equal_counts.append(b_count == l_count)
                    else:
                        sanity_equal_counts.append(None)

                # averages
                base_ok = [x for x in base_lat if x is not None]
                lift_ok = [x for x in lift_lat if x is not None]

                base_avg = round(sum(base_ok)/len(base_ok), 3) if base_ok else ""
                lift_avg = round(sum(lift_ok)/len(lift_ok), 3) if lift_ok else ""

                base_build_avg = round(sum(per_run_base_build)/len(per_run_base_build), 3)
                lift_build_avg = round(sum(per_run_lift_build)/len(per_run_lift_build), 3)

                sanity_ok = sum(1 for x in sanity_equal_counts if x is True)
                sanity_bad = sum(1 for x in sanity_equal_counts if x is False)
                sanity_all_equal = 1 if (sanity_bad == 0 and sanity_ok > 0) else 0

                if isinstance(base_avg, (int,float)) and isinstance(lift_avg, (int,float)) and isinstance(lift_build_avg,(int,float)) and lift_build_avg+lift_avg>0:
                    speedup = round(base_avg / (lift_build_avg + lift_avg), 3)
                else:
                    speedup = ""

                results.append({
                    "edges": E,
                    "hops": H,
                    "runs": args.repeats,
                    "baseline_build_ms": base_build_avg,
                    "lifted_build_ms": lift_build_avg,
                    "baseline_timeouts": base_timeouts,
                    "baseline_successes": len(base_ok),
                    "baseline_avg_latency_ms": base_avg,
                    "lifted_timeouts": lift_timeouts,
                    "lifted_successes": len(lift_ok),
                    "lifted_avg_latency_ms": lift_avg,
                    "speedup": speedup,
                    "sanity_ok_runs": sanity_ok,
                    "sanity_mismatch_runs": sanity_bad,
                    "sanity_all_equal": sanity_all_equal,
                })

                print(f"E={E}, H={H}: baseline_avg={base_avg}ms, lifted_build_avg={lift_build_avg}ms, lifted_avg={lift_avg}ms, speedup={speedup}")

    # Write CSV
    fields = [
        "edges","hops","runs",
        "baseline_build_ms","lifted_build_ms",
        "baseline_timeouts","baseline_successes","baseline_avg_latency_ms",
        "lifted_timeouts","lifted_successes","lifted_avg_latency_ms",
        "speedup",
        "sanity_ok_runs","sanity_mismatch_runs","sanity_all_equal",
    ]
    with open(out_path,"w",newline="") as f:
        writer = csv.DictWriter(f,fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWrote results to {out_path}")


if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass
    main()
