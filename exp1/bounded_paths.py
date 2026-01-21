#!/usr/bin/env python3
import argparse
import csv
import sys
import time
import multiprocessing
import random
import subprocess
import os
import glob
import statistics
from datetime import datetime
from pathlib import Path
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# python3 ./bounded_paths.py --password "ItayBachar88"
# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------

SETUP_TIMEOUT_SEC = 600.0 
LOAD_BATCH_SIZE = 50000      # Batch size for loading CSV
LIFT_BATCH_SIZE = 5000       # Batch size for Lifted Graph creation
DELETE_BATCH_SIZE = 10000    # Batch size for cleaning the DB

# -----------------------------------------------------------------------------
# Cypher Queries
# -----------------------------------------------------------------------------

INIT_CONSTRAINTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level)",
]

LOAD_ACCOUNTS = "UNWIND $ids AS i MERGE (:Account {id: i})"

LOAD_EDGES = """
UNWIND $rows AS r
MATCH (u:Account {id: r.src})
MATCH (v:Account {id: r.dst})
CREATE (u)-[:TRANSFER {
  amount: toInteger(r.label),
  ts: datetime("2025-01-01") + duration({days: toInteger(rand()*365)})
}]->(v)
"""

GET_ALL_ACCOUNTS = "MATCH (a:Account) RETURN a.id AS id"

BUILD_STAGE_NODES_BATCH = """
UNWIND $batchIds AS aid
MATCH (v:Account {id: aid})
OPTIONAL MATCH (:Account)-[e:TRANSFER]->(v)
WITH v, collect(DISTINCT e.amount) AS inAmts
WITH v, [-1] + [amt IN inAmts WHERE amt IS NOT NULL] AS levels
UNWIND levels AS level
MERGE (:Stage {accId: v.id, level: level})
"""

BUILD_STAGE_EDGES_BATCH = """
UNWIND $batchIds AS aid
MATCH (u:Account {id: aid})-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
MATCH (su:Stage {accId: u.id})
WHERE su.level < j
MERGE (sv:Stage {accId: v.id, level: j})
MERGE (su)-[le:TRANSFER_LIFT {amount: j}]->(sv)
  ON CREATE SET le.ts = ts
"""

# -----------------------------------------------------------------------------
# Query Templates
# -----------------------------------------------------------------------------

def make_baseline_query(max_hops: int) -> str:
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

def make_lifted_query(max_hops: int) -> str:
    return f"""
CALL(){{
  MATCH (start:Stage {{level: -1}})
  MATCH p = (start)-[:TRANSFER_LIFT*1..{max_hops}]->(x:Stage)
  RETURN 1 AS row
}}
RETURN count(*) AS rows
"""

# -----------------------------------------------------------------------------
# gMark Integration
# -----------------------------------------------------------------------------

def run_shell(cmd, cwd=None):
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stderr}")

def update_gmark_schema(schema_path, n_edges, n_nodes):
    edges_per_node = n_edges / n_nodes if n_nodes > 0 else 1
    val = max(1, int(round(edges_per_node))) if edges_per_node >= 0.5 else 1

    xml_content = f'''<generator>
  <graph><nodes>0</nodes></graph>
  <predicates><size>{n_edges}</size><alias symbol="0">transfer</alias></predicates>
  <types><size>1</size><alias type="0">Account</alias></types>
  <schema>
    <source type="0">
      <target type="0" symbol="0" multiplicity="*">
        <indistribution type="uniform"><min>{val}</min><max>{val}</max></indistribution>
        <outdistribution type="uniform"><min>{val}</min><max>{val}</max></outdistribution>
      </target>
    </source>
  </schema>
  <workload id="0" size="0"/>
</generator>'''
    
    with open(schema_path, "w") as f:
        f.write(xml_content)

def generate_gmark_csv(n_nodes, n_edges, gmark_dir, output_dir, schema_file="shop.xml"):
    print(f"[DEBUG] Generating gMark graph: {n_nodes} nodes, {n_edges} edges...")
    src_dir = os.path.join(gmark_dir, "src")
    schema_path = os.path.join(gmark_dir, f"use-cases/{schema_file}")
    
    update_gmark_schema(schema_path, n_edges, n_nodes)
    
    run_shell([
        "./test", "-c", f"../use-cases/{schema_file}", "-g", "../demo/play/play-graph.txt",
        "-w", "../demo/play/play-workload.xml", "-r", "../demo/play", "-n", str(n_nodes)
    ], cwd=src_dir)

    is_sparse = (n_edges / n_nodes) < 0.5
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_path = os.path.join(output_dir, "edges.csv")
    
    with open(csv_path, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["src", "dst", "amount"])
        
        if is_sparse:
            nodes = list(range(n_nodes))
            count = 0
            while count < n_edges:
                src, dst = random.choice(nodes), random.choice(nodes)
                if src != dst:
                    writer.writerow([src, dst, random.randint(1, 1000)])
                    count += 1
        else:
            files = sorted(glob.glob(os.path.join(src_dir, "../demo/play/play-graph.txt*")))
            for gf in files:
                with open(gf, "r") as fin:
                    for line in fin:
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            writer.writerow([parts[0], parts[2], random.randint(1, 1000)])
    print(f"[DEBUG] gMark CSV generated at {csv_path}")
    return csv_path

# -----------------------------------------------------------------------------
# Neo4j Operations
# -----------------------------------------------------------------------------

def run_write(session, cypher, params=None, desc="Query"):
    t0 = time.time()
    def work(tx):
        tx.run(cypher, params or {}, timeout=SETUP_TIMEOUT_SEC).consume()
    session.execute_write(work)

def clear_database(session):
    """Deletes all nodes and relationships in batches to avoid OOM."""
    print("[DEBUG] Clearing database (Batched Deletion)...")
    total_deleted = 0
    while True:
        # Delete a batch of nodes (and their relationships)
        query = f"""
        MATCH (n)
        WITH n LIMIT {DELETE_BATCH_SIZE}
        DETACH DELETE n
        RETURN count(n) as count
        """
        result = session.run(query).single()
        count = result["count"]
        total_deleted += count
        
        if count == 0:
            break
        
    print(f"[DEBUG] Database cleared. Total nodes removed: {total_deleted}")

def load_graph_to_neo4j(session, csv_path):
    print("[DEBUG] Reading CSV for Neo4j loading...")
    edges = []
    nodes = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u, v = int(row["src"]), int(row["dst"])
            edges.append({"src": u, "dst": v, "label": int(row["amount"])})
            nodes.add(u)
            nodes.add(v)
    
    node_list = sorted(list(nodes))
    print(f"[DEBUG] CSV Loaded. Found {len(node_list)} nodes and {len(edges)} edges.")

    print(f"[DEBUG] Loading {len(node_list)} Accounts...")
    run_write(session, LOAD_ACCOUNTS, {"ids": node_list}, desc="LOAD_ACCOUNTS")

    total_edges = len(edges)
    print(f"[DEBUG] Loading {total_edges} Edges in batches of {LOAD_BATCH_SIZE}...")
    for i in range(0, total_edges, LOAD_BATCH_SIZE):
        batch = edges[i:i + LOAD_BATCH_SIZE]
        run_write(session, LOAD_EDGES, {"rows": batch}, desc=f"LOAD_EDGES_BATCH_{i}")

def build_infrastructure(session, csv_path, timeout=None):
    print("\n[DEBUG] --- STARTING INFRASTRUCTURE BUILD ---")
    
    # 1. Clear Database (Batched)
    clear_database(session)
    
    # 2. Re-apply Constraints
    for i, stmt in enumerate(INIT_CONSTRAINTS):
        run_write(session, stmt, desc=f"CONSTRAINT_{i}")

    # 3. Base Graph Load
    t_start = time.perf_counter()
    load_graph_to_neo4j(session, csv_path)
    base_ms = (time.perf_counter() - t_start) * 1000
    print(f"[DEBUG] Base Graph Load Complete. Time: {base_ms:.2f}ms")

    # 4. Lifted Graph Build (Batched)
    print("\n[DEBUG] --- STARTING LIFTED GRAPH GENERATION ---")
    t_lift_start = time.perf_counter()
    
    result = session.run(GET_ALL_ACCOUNTS)
    all_ids = [record["id"] for record in result]
    total_ids = len(all_ids)
    print(f"[DEBUG] Fetched {total_ids} Account IDs for processing.")

    # Build Stage Nodes
    print(f"[DEBUG] Building Stage Nodes in batches of {LIFT_BATCH_SIZE}...")
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        # Timeout Check
        if timeout and (time.perf_counter() - t_lift_start > timeout):
            print(f"[DEBUG] Build Timed Out during Node generation after {time.perf_counter() - t_lift_start:.2f}s")
            return base_ms, None

        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, BUILD_STAGE_NODES_BATCH, {"batchIds": batch_ids}, desc=f"LIFT_NODES_{i}")
        if i % (LIFT_BATCH_SIZE * 5) == 0:
            print(f"  [DEBUG] Progress: {i}/{total_ids} nodes processed...")

    # Build Stage Edges
    print(f"[DEBUG] Building Stage Edges in batches of {LIFT_BATCH_SIZE}...")
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        # Timeout Check
        if timeout and (time.perf_counter() - t_lift_start > timeout):
            print(f"[DEBUG] Build Timed Out during Edge generation after {time.perf_counter() - t_lift_start:.2f}s")
            return base_ms, None

        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, BUILD_STAGE_EDGES_BATCH, {"batchIds": batch_ids}, desc=f"LIFT_EDGES_{i}")
        if i % (LIFT_BATCH_SIZE * 5) == 0:
            print(f"  [DEBUG] Progress: {i}/{total_ids} source nodes processed...")
    
    lift_ms = (time.perf_counter() - t_lift_start) * 1000
    print(f"[DEBUG] Lifted Graph Build Complete. Time: {lift_ms:.2f}ms")

    return base_ms, lift_ms

# -----------------------------------------------------------------------------
# Worker / Main
# -----------------------------------------------------------------------------

def _worker(conn, uri, auth, cypher, database):
    try:
        driver = GraphDatabase.driver(uri, auth=auth)
        with driver.session(database=database) as s:
            t0 = time.perf_counter()
            res = s.run(cypher).single()
            dur = time.perf_counter() - t0
            val = res[0] if res else 0
        conn.send(("ok", (val, dur)))
    except Exception as e:
        conn.send(("error", str(e)))
    finally:
        conn.close()

def run_timed_query(uri, auth, cypher, timeout, database):
    parent, child = multiprocessing.Pipe()
    p = multiprocessing.Process(target=_worker, args=(child, uri, auth, cypher, database))
    p.start()
    
    if parent.poll(timeout):
        status, data = parent.recv()
        p.join()
        if status == "ok":
            return data
        raise Neo4jError("ClientError", data)
    else:
        p.terminate()
        p.join()
        raise TimeoutError(f"Query exceeded {timeout}s")

def init_results_file(filename):
    HEADERS = [
        ("Nodes", 8), ("Density", 7), ("Edges", 9), ("Hops", 5),
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5),
        ("Speedup", 15), ("Sanity", 8)
    ]
    fmt = "  ".join([f"{{:<{w}}}" for _, w in HEADERS])
    title_row = fmt.format(*[h[0] for h in HEADERS])
    separator = "  ".join(["-" * w for _, w in HEADERS])
    with open(filename, "w") as f:
        f.write(title_row + "\n")
        f.write(separator + "\n")

def append_result_row(filename, r):
    HEADERS = [
        ("Nodes", 8), ("Density", 7), ("Edges", 9), ("Hops", 5),
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5),
        ("Speedup", 15), ("Sanity", 8)
    ]
    fmt = "  ".join([f"{{:<{w}}}" for _, w in HEADERS])
    
    # Handle Speedup formatting (string "x" vs float)
    speedup_val = r["speedup"]
    if isinstance(speedup_val, str):
        speedup_str = speedup_val
    else:
        speedup_str = f"{speedup_val:.2f}"
    
    line = fmt.format(
        r["nodes"], r["density"], r["edges"], r["hops"],
        f"{r['base_mean']:.2f}", f"{r['base_std']:.2f}", r['base_timeouts'],
        f"{r['build_avg']:.2f}",
        f"{r['lift_mean']:.2f}", f"{r['lift_std']:.2f}", r['lift_timeouts'],
        speedup_str,
        r["sanity"]
    )
    with open(filename, "a") as f:
        f.write(line + "\n")

def main():
    parser = argparse.ArgumentParser(description="Clean Baseline vs Lifted Benchmark")
    parser.add_argument("--uri", default="bolt://127.0.0.1:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j", help="Name of the database")
    parser.add_argument("--densities", type=float, nargs="+", default=[1.0, 5.0, 10.0])
    parser.add_argument("--hops", type=int, nargs="+", default=[2,4,8,16])
    parser.add_argument("--nodes", type=int, nargs="+", default=[100, 1000, 10000, 100000, 1000000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--gmark_dir", default="../gmark")
    parser.add_argument("--out", default="results/bounded_paths") 
    args = parser.parse_args()

    auth = (args.user, args.password)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    out_dir = os.path.dirname(args.out)
    if out_dir:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    final_output_file = f"{args.out}_{timestamp}.txt"
    print(f"Output will be saved to: {final_output_file}")
    init_results_file(final_output_file)

    try:
        with GraphDatabase.driver(args.uri, auth=auth) as d:
            d.verify_connectivity()
            print(f"[DEBUG] Connection to Neo4j successful (DB: {args.database})")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    for N in args.nodes:
        print(f"\n=== PROCESSING NODE COUNT: {N} ===")
        for D in args.densities:
            E = int(N * D)
            print(f"\n--- Experiment: Nodes={N}, Density={D} (Edges={E}) ---")
            
            driver = GraphDatabase.driver(args.uri, auth=auth)
            
            # Storage for aggregation across repeats
            experiment_data = {
                h: {
                    "base": [], "lift": [], "build": [], 
                    "base_to": 0, "lift_to": 0, "build_to": 0,
                    "sanity_mismatch": False, 
                    "sanity_verified_count": 0
                } 
                for h in args.hops
            }

            for r in range(args.repeats):
                print(f"\n[DEBUG] --- Run {r+1}/{args.repeats} ---")
                tmp_dir = f"/tmp/exp_run_{r}"
                csv_path = generate_gmark_csv(N, E, args.gmark_dir, tmp_dir)
                
                try:
                    # 1. Build Infrastructure with TIMEOUT check
                    with driver.session(database=args.database) as session:
                        _, lift_ms = build_infrastructure(session, csv_path, timeout=args.timeout)
                    
                    lift_build_failed = (lift_ms is None)
                    if lift_build_failed:
                        print(f"    [WARN] Leveled Build Timed Out (> {args.timeout}s). Skipping Lifted Queries.")

                    # 2. Test ALL hops on this same infrastructure
                    for H in args.hops:
                        print(f"  > Testing Hops={H} (on existing graph)...")
                        
                        if lift_build_failed:
                            experiment_data[H]["build_to"] += 1
                        else:
                            experiment_data[H]["build"].append(lift_ms)

                        base_count = None
                        lifted_count = None

                        # Baseline
                        try:
                            val, t_sec = run_timed_query(args.uri, auth, make_baseline_query(H), args.timeout, args.database)
                            experiment_data[H]["base"].append(t_sec * 1000)
                            base_count = val
                            print(f"    Baseline: {t_sec*1000:.2f}ms | Count: {base_count}")
                        except TimeoutError:
                            print(f"    Baseline: TIMEOUT")
                            experiment_data[H]["base_to"] += 1
                        except Exception as e:
                            print(f"    Baseline Error: {e}")

                        # Lifted (Only if build succeeded)
                        if lift_build_failed:
                            print(f"    Lifted:   SKIPPING (Build Timeout)")
                        else:
                            try:
                                val, t_sec = run_timed_query(args.uri, auth, make_lifted_query(H), args.timeout, args.database)
                                experiment_data[H]["lift"].append(t_sec * 1000)
                                lifted_count = val
                                print(f"    Lifted:   {t_sec*1000:.2f}ms | Count: {lifted_count}")
                            except TimeoutError:
                                print(f"    Lifted:   TIMEOUT")
                                experiment_data[H]["lift_to"] += 1
                            except Exception as e:
                                print(f"    Lifted Error: {e}")

                        # Sanity Check
                        if base_count is not None and lifted_count is not None:
                            if base_count == lifted_count:
                                print(f"    [SANITY] PASS (Count: {base_count})")
                                experiment_data[H]["sanity_verified_count"] += 1
                            else:
                                print(f"    [SANITY] FAIL! Base={base_count} vs Lifted={lifted_count}")
                                experiment_data[H]["sanity_mismatch"] = True
                        else:
                            print(f"    [SANITY] N/A (Timeout or Skip)")

                except Exception as e:
                    print(f"\n[CRITICAL ERROR] Run failed: {e}")
                    
                finally:
                    try: os.remove(csv_path)
                    except: pass

            # Aggregate and Save Results for this Density
            print(f"\n--- Summary for Nodes={N}, Density={D} ---")
            for H in args.hops:
                data = experiment_data[H]
                
                def get_stats(vals):
                    if not vals: return 0.0, 0.0
                    if len(vals) == 1: return vals[0], 0.0
                    return statistics.mean(vals), statistics.stdev(vals)

                b_mean, b_std = get_stats(data["base"])
                l_mean, l_std = get_stats(data["lift"])
                build_avg = statistics.mean(data["build"]) if data["build"] else 0.0
                
                # Check for Timeout/Failure conditions
                # Lift is considered failed if it timed out in Query OR Build
                total_lift_failures = data["lift_to"] + data["build_to"]
                lift_failed = (len(data["lift"]) == 0 and total_lift_failures > 0)
                
                base_failed = (len(data["base"]) == 0 and data["base_to"] > 0)

                denominator = build_avg + l_mean
                
                if base_failed and lift_failed:
                    speedup = "Both Time Out"
                elif lift_failed and not base_failed:
                    speedup = "0.00"
                elif base_failed and denominator > 0:
                    speedup = float('inf')
                elif denominator > 0:
                    speedup = b_mean / denominator
                else:
                    speedup = 0.0

                # Sanity String
                if data["sanity_mismatch"]:
                    sanity_str = "FAIL"
                elif data["sanity_verified_count"] > 0:
                    sanity_str = "PASS"
                else:
                    sanity_str = "N/A"

                # Handle formatting for print
                if isinstance(speedup, str):
                    speedup_str = speedup
                else:
                    speedup_str = f"{speedup:.2f}x"

                row = {
                    "nodes": N, "density": D, "edges": E, "hops": H,
                    "base_mean": b_mean, "base_std": b_std, "base_timeouts": data["base_to"],
                    "build_avg": build_avg,
                    "lift_mean": l_mean, "lift_std": l_std, "lift_timeouts": total_lift_failures,
                    "speedup": speedup,
                    "sanity": sanity_str
                }
                append_result_row(final_output_file, row)
                print(f"  Hops={H} | Base={b_mean:.2f}ms | Lift={l_mean:.2f}ms | Speedup={speedup_str} | Sanity={sanity_str}")

            driver.close()

    print(f"\nDone. Final results saved to {final_output_file}")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
