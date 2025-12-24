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

# Example usage:
# python3 ./peak_paths.py --password "ItayBachar88" --nodes 100 1000 10000 100000 1000000

# -----------------------------------------------------------------------------
# Global Debug Control
# -----------------------------------------------------------------------------
DEBUG = False

def debug_print(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------

SETUP_TIMEOUT_SEC = 900.0  # Increased for complex motif build
LOAD_BATCH_SIZE = 50000    
LIFT_BATCH_SIZE = 5000     
DELETE_BATCH_SIZE = 10000  

# -----------------------------------------------------------------------------
# Cypher Queries (PEAK MOTIF: UP -> DOWN)
# -----------------------------------------------------------------------------

INIT_CONSTRAINTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level_layer IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level, s.layer)",
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

# Build Nodes for both Layer 1 (Up) and Layer 2 (Down)
BUILD_STAGE_NODES_BATCH = """
UNWIND $batchIds AS aid
MATCH (v:Account {id: aid})
OPTIONAL MATCH (:Account)-[e:TRANSFER]->(v)
WITH v, collect(DISTINCT e.amount) AS inAmts
WITH v, [-1] + [amt IN inAmts WHERE amt IS NOT NULL] AS levels
UNWIND levels AS level
UNWIND [1, 2] AS layer
MERGE (:Stage {accId: v.id, level: level, layer: layer})
"""

# Build Edges according to Peak Logic (Up -> Switch -> Down)
# FIXED: Replaced deprecated id(e) with elementId(e)
BUILD_STAGE_EDGES_BATCH = """
UNWIND $batchIds AS aid
MATCH (u:Account {id: aid})-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts, elementId(e) as eid

// 1. Intra-motif: Layer 1 (Up) -> Layer 1 (Up)
MATCH (su1:Stage {accId: u.id, layer: 1})
WHERE su1.level < j
MATCH (sv1:Stage {accId: v.id, level: j, layer: 1})
MERGE (su1)-[le1:TRANSFER_LIFT {amount: j}]->(sv1)
  ON CREATE SET le1.ts = ts, le1.eid = eid  // <--- STORE EID

// 2. Motif Switching: Layer 1 (Up) -> Layer 2 (Down)
WITH u, v, e, j, ts, eid
MATCH (su1_sw:Stage {accId: u.id, layer: 1})
WHERE su1_sw.level > j AND su1_sw.level <> -1
MATCH (sv2_sw:Stage {accId: v.id, level: j, layer: 2})
MERGE (su1_sw)-[le_sw:TRANSFER_LIFT {amount: j}]->(sv2_sw)
  ON CREATE SET le_sw.ts = ts, le_sw.eid = eid // <--- STORE EID

// 3. Intra-motif: Layer 2 (Down) -> Layer 2 (Down)
WITH u, v, e, j, ts, eid
MATCH (su2:Stage {accId: u.id, layer: 2})
WHERE su2.level > j
MATCH (sv2:Stage {accId: v.id, level: j, layer: 2})
MERGE (su2)-[le2:TRANSFER_LIFT {amount: j}]->(sv2)
  ON CREATE SET le2.ts = ts, le2.eid = eid // <--- STORE EID
"""

# -----------------------------------------------------------------------------
# Query Templates (PEAK)
# -----------------------------------------------------------------------------

def make_baseline_query_peak(max_hops_str: str) -> str:
    range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
    return f"""
CALL(){{
  MATCH p = (s:Account)-[:TRANSFER{range_str}]->(t:Account)
  WITH s, p, [r IN relationships(p) | r.amount] AS amts, t
  WHERE size(amts) >= 2 
    AND ANY(k IN range(0, size(amts)-2) WHERE
      ALL(i IN range(1, k) WHERE amts[i-1] < amts[i])
      AND amts[k] > amts[k+1]
      AND ALL(i IN range(k+2, size(amts)-1) WHERE amts[i-1] > amts[i])
    )
  RETURN 1 AS row
}}
RETURN count(*) AS rows
"""

def make_lifted_query_peak(max_hops_str: str) -> str:
    range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
    return f"""
CALL(){{
  MATCH (start:Stage {{level: -1, layer: 1}})
  MATCH p = (start)-[:TRANSFER_LIFT{range_str}]->(x:Stage {{layer: 2}})
  WHERE NOT ANY(r1 IN relationships(p) 
        WHERE size([r2 IN relationships(p) WHERE r2.eid = r1.eid]) > 1)
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
    debug_print(f"[DEBUG] Generating gMark graph: {n_nodes} nodes, {n_edges} edges...")
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
    debug_print(f"[DEBUG] gMark CSV generated at {csv_path}")
    return csv_path

# -----------------------------------------------------------------------------
# Neo4j Operations
# -----------------------------------------------------------------------------

def run_write(session, cypher, params=None, desc="Query"):
    def work(tx):
        tx.run(cypher, params or {}, timeout=SETUP_TIMEOUT_SEC).consume()
    session.execute_write(work)

def clear_database(session):
    debug_print("[DEBUG] Clearing database (Batched Deletion)...")
    total_deleted = 0
    while True:
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
    debug_print(f"[DEBUG] Database cleared. Total nodes removed: {total_deleted}")

def load_graph_to_neo4j(session, csv_path):
    debug_print("[DEBUG] Reading CSV for Neo4j loading...")
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
    debug_print(f"[DEBUG] CSV Loaded. Found {len(node_list)} nodes and {len(edges)} edges.")

    run_write(session, LOAD_ACCOUNTS, {"ids": node_list}, desc="LOAD_ACCOUNTS")

    total_edges = len(edges)
    debug_print(f"[DEBUG] Loading {total_edges} Edges in batches...")
    for i in range(0, total_edges, LOAD_BATCH_SIZE):
        batch = edges[i:i + LOAD_BATCH_SIZE]
        run_write(session, LOAD_EDGES, {"rows": batch}, desc=f"LOAD_EDGES_BATCH_{i}")

def build_infrastructure(session, csv_path):
    debug_print("\n[DEBUG] --- STARTING INFRASTRUCTURE BUILD (PEAK) ---")
    
    clear_database(session)
    for i, stmt in enumerate(INIT_CONSTRAINTS):
        run_write(session, stmt, desc=f"CONSTRAINT_{i}")

    t_start = time.perf_counter()
    load_graph_to_neo4j(session, csv_path)
    base_ms = (time.perf_counter() - t_start) * 1000
    debug_print(f"[DEBUG] Base Graph Load Complete. Time: {base_ms:.2f}ms")

    debug_print("\n[DEBUG] --- STARTING LIFTED GRAPH GENERATION ---")
    t_start = time.perf_counter()
    
    result = session.run(GET_ALL_ACCOUNTS)
    all_ids = [record["id"] for record in result]
    total_ids = len(all_ids)
    
    debug_print(f"[DEBUG] Building Stage Nodes (Layers 1 & 2)...")
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, BUILD_STAGE_NODES_BATCH, {"batchIds": batch_ids}, desc=f"LIFT_NODES_{i}")
        if i % (LIFT_BATCH_SIZE * 5) == 0:
            debug_print(f"  [DEBUG] Progress: {i}/{total_ids} nodes...")

    debug_print(f"[DEBUG] Building Stage Edges (Up->Switch->Down)...")
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, BUILD_STAGE_EDGES_BATCH, {"batchIds": batch_ids}, desc=f"LIFT_EDGES_{i}")
        if i % (LIFT_BATCH_SIZE * 5) == 0:
            debug_print(f"  [DEBUG] Progress: {i}/{total_ids} source nodes...")
    
    lift_ms = (time.perf_counter() - t_start) * 1000
    debug_print(f"[DEBUG] Lifted Graph Build Complete. Time: {lift_ms:.2f}ms")

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
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5), ("Paths(B)", 10),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5), ("Paths(L)", 10),
        ("Speedup", 8), ("Sanity", 8)
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
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5), ("Paths(B)", 10),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5), ("Paths(L)", 10),
        ("Speedup", 8), ("Sanity", 8)
    ]
    fmt = "  ".join([f"{{:<{w}}}" for _, w in HEADERS])
    line = fmt.format(
        r["nodes"], r["density"], r["edges"], str(r["hops"]),
        f"{r['base_mean']:.2f}", f"{r['base_std']:.2f}", r['base_timeouts'], f"{r['base_paths']:.1f}",
        f"{r['build_avg']:.2f}",
        f"{r['lift_mean']:.2f}", f"{r['lift_std']:.2f}", r['lift_timeouts'], f"{r['lift_paths']:.1f}",
        f"{r['speedup']:.2f}",
        r["sanity"]
    )
    with open(filename, "a") as f:
        f.write(line + "\n")

def main():
    global DEBUG
    
    parser = argparse.ArgumentParser(description="Clean Baseline vs Lifted Peak Motif (Bounded + Unbounded)")
    parser.add_argument("--uri", default="bolt://127.0.0.1:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j", help="Name of the database")
    parser.add_argument("--densities", type=float, nargs="+", default=[1.0, 5.0, 10.0])
    parser.add_argument("--hops", type=int, nargs="+", default=[2,4,8,16,32,64])
    parser.add_argument("--nodes", type=int, nargs="+", default=[1000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--gmark_dir", default="../gmark")
    parser.add_argument("--out", default="results/peak_paths")
    parser.add_argument("--debug", action="store_true", help="Enable debug logs") 
    args = parser.parse_args()

    DEBUG = args.debug # Set global debug flag based on argument

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
            debug_print(f"[DEBUG] Connection to Neo4j successful (DB: {args.database})")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    for N in args.nodes:
        print(f"\n=== PROCESSING NODE COUNT: {N} ===")
        for D in args.densities:
            E = int(N * D)
            print(f"\n--- Experiment: Nodes={N}, Density={D} (Edges={E}) ---")
            
            driver = GraphDatabase.driver(args.uri, auth=auth)
            
            # Configurations to run: All numeric hops + "inf" (Unbounded)
            configs = list(args.hops) + ["inf"]
            
            experiment_data = {
                h: {
                    "base": [], "lift": [], "build": [], 
                    "base_counts": [], "lift_counts": [], 
                    "base_to": 0, "lift_to": 0,
                    "sanity_mismatch": False, 
                    "sanity_verified_count": 0
                } 
                for h in configs
            }

            for r in range(args.repeats):
                debug_print(f"\n[DEBUG] --- Run {r+1}/{args.repeats} ---")
                tmp_dir = f"/tmp/exp_run_{r}"
                csv_path = generate_gmark_csv(N, E, args.gmark_dir, tmp_dir)
                
                try:
                    # 1. Build Peak Infrastructure ONCE for this repeat
                    with driver.session(database=args.database) as session:
                        _, lift_ms = build_infrastructure(session, csv_path)
                    
                    # Track consecutive timeouts for this specific graph instance
                    consecutive_base_to = 0
                    consecutive_lift_to = 0

                    # 2. Test Bounded Hops AND Unbounded
                    for H in configs:
                        h_label = f"Unbounded" if H == "inf" else f"Hops={H}"
                        print(f"  > Testing {h_label}...")
                        
                        experiment_data[H]["build"].append(lift_ms)

                        base_count = None
                        lifted_count = None

                        # Baseline
                        if consecutive_base_to >= 2:
                            print(f"    Baseline: SKIPPING (Consecutive Timeouts)")
                            experiment_data[H]["base_to"] += 1
                        else:
                            query_base = make_baseline_query_peak(str(H))
                            try:
                                val, t_sec = run_timed_query(args.uri, auth, query_base, args.timeout, args.database)
                                experiment_data[H]["base"].append(t_sec * 1000)
                                experiment_data[H]["base_counts"].append(val)
                                base_count = val
                                print(f"    Baseline: {t_sec*1000:.2f}ms | Count: {base_count}")
                                consecutive_base_to = 0  # Reset on success
                            except TimeoutError:
                                print(f"    Baseline: TIMEOUT")
                                experiment_data[H]["base_to"] += 1
                                consecutive_base_to += 1
                            except Exception as e:
                                print(f"    Baseline Error: {e}")

                        # Lifted
                        if consecutive_lift_to >= 2:
                            print(f"    Lifted:   SKIPPING (Consecutive Timeouts)")
                            experiment_data[H]["lift_to"] += 1
                        else:
                            query_lift = make_lifted_query_peak(str(H))
                            try:
                                val, t_sec = run_timed_query(args.uri, auth, query_lift, args.timeout, args.database)
                                experiment_data[H]["lift"].append(t_sec * 1000)
                                experiment_data[H]["lift_counts"].append(val)
                                lifted_count = val
                                print(f"    Lifted:   {t_sec*1000:.2f}ms | Count: {lifted_count}")
                                consecutive_lift_to = 0  # Reset on success
                            except TimeoutError:
                                print(f"    Lifted:   TIMEOUT")
                                experiment_data[H]["lift_to"] += 1
                                consecutive_lift_to += 1
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

            # Aggregate Results
            print(f"\n--- Summary for Nodes={N}, Density={D} ---")
            for H in configs:
                data = experiment_data[H]
                
                def get_stats(vals):
                    if not vals: return 0.0, 0.0
                    if len(vals) == 1: return vals[0], 0.0
                    return statistics.mean(vals), statistics.stdev(vals)

                b_mean, b_std = get_stats(data["base"])
                l_mean, l_std = get_stats(data["lift"])
                build_avg = statistics.mean(data["build"]) if data["build"] else 0.0
                
                # Calculate average paths (from successful runs only)
                base_paths_avg = statistics.mean(data["base_counts"]) if data["base_counts"] else 0.0
                lift_paths_avg = statistics.mean(data["lift_counts"]) if data["lift_counts"] else 0.0

                denominator = build_avg + l_mean
                
                if len(data["base"]) == 0 and data["base_to"] > 0 and denominator > 0:
                    speedup = float('inf')
                elif denominator > 0:
                    speedup = b_mean / denominator
                else:
                    speedup = 0.0

                if data["sanity_mismatch"]: sanity_str = "FAIL"
                elif data["sanity_verified_count"] > 0: sanity_str = "PASS"
                else: sanity_str = "N/A"

                row = {
                    "nodes": N, "density": D, "edges": E, "hops": H,
                    "base_mean": b_mean, "base_std": b_std, "base_timeouts": data["base_to"],
                    "base_paths": base_paths_avg,
                    "build_avg": build_avg,
                    "lift_mean": l_mean, "lift_std": l_std, "lift_timeouts": data["lift_to"],
                    "lift_paths": lift_paths_avg,
                    "speedup": speedup,
                    "sanity": sanity_str
                }
                append_result_row(final_output_file, row)
                print(f"  Hops={H} | Base={b_mean:.2f}ms ({base_paths_avg:.0f} paths) | Lift={l_mean:.2f}ms ({lift_paths_avg:.0f} paths) | Speedup={speedup:.2f}x")

            driver.close()

    print(f"\nDone. Final results saved to {final_output_file}")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
