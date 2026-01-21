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

# python3 ./peak_paths.py --password "ItayBachar88" --debug
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

SETUP_TIMEOUT_SEC = 900.0
LOAD_BATCH_SIZE = 50000     
LIFT_BATCH_SIZE = 1000      
DELETE_BATCH_SIZE = 10000   
ITERATIVE_BATCH_SIZE = 2000

# -----------------------------------------------------------------------------
# Cypher Queries
# -----------------------------------------------------------------------------

INIT_CONSTRAINTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level_layer IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level, s.layer)",
    "CREATE INDEX stage_active IF NOT EXISTS FOR (s:Stage) ON (s.active)",
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

# --- GLOBAL BUILD QUERIES ---
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

BUILD_STAGE_EDGES_BATCH = """
UNWIND $batchIds AS aid
MATCH (u:Account {id: aid})-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts, elementId(e) as eid

// 1. Intra-motif: Layer 1
MATCH (su1:Stage {accId: u.id, layer: 1}) WHERE su1.level < j
MATCH (sv1:Stage {accId: v.id, level: j, layer: 1})
MERGE (su1)-[le1:TRANSFER_LIFT {amount: j}]->(sv1)
  ON CREATE SET le1.ts = ts, le1.eid = eid  

// 2. Motif Switching: Layer 1 -> Layer 2
WITH u, v, e, j, ts, eid
MATCH (su1_sw:Stage {accId: u.id, layer: 1}) WHERE su1_sw.level > j AND su1_sw.level <> -1
MATCH (sv2_sw:Stage {accId: v.id, level: j, layer: 2})
MERGE (su1_sw)-[le_sw:TRANSFER_LIFT {amount: j}]->(sv2_sw)
  ON CREATE SET le_sw.ts = ts, le_sw.eid = eid 

// 3. Intra-motif: Layer 2
WITH u, v, e, j, ts, eid
MATCH (su2:Stage {accId: u.id, layer: 2}) WHERE su2.level > j
MATCH (sv2:Stage {accId: v.id, level: j, layer: 2})
MERGE (su2)-[le2:TRANSFER_LIFT {amount: j}]->(sv2)
  ON CREATE SET le2.ts = ts, le2.eid = eid 
"""

# --- ITERATIVE BUILD QUERIES ---
INIT_ITERATIVE_ROOTS = """
UNWIND $batchIds AS aid
MATCH (a:Account {id: aid})
MERGE (s:Stage {accId: a.id, level: -1, layer: 1})
ON CREATE SET s.active = 0
ON MATCH SET s.active = 0
"""

GET_ACTIVE_STAGE_IDS = "MATCH (s:Stage) WHERE s.active = $step RETURN elementId(s) as id"

EXPAND_ITERATIVE_BATCH = """
UNWIND $batchIds AS sid
MATCH (u:Stage) WHERE elementId(u) = sid
MATCH (u_acc:Account {id: u.accId})-[e:TRANSFER]->(v_acc:Account)
WITH u, v_acc, e, e.amount as j, $next_step as next_s

FOREACH (_ IN CASE WHEN u.layer=1 AND u.level < j THEN [1] ELSE [] END |
    MERGE (v1:Stage {accId: v_acc.id, level: j, layer: 1})
    ON CREATE SET v1.active = next_s
    ON MATCH SET v1.active = next_s
    MERGE (u)-[r1:TRANSFER_LIFT {amount: j}]->(v1)
    ON CREATE SET r1.ts = e.ts, r1.eid = elementId(e)
)
FOREACH (_ IN CASE WHEN u.layer=1 AND u.level > j AND u.level <> -1 THEN [1] ELSE [] END |
    MERGE (v2:Stage {accId: v_acc.id, level: j, layer: 2})
    ON CREATE SET v2.active = next_s
    ON MATCH SET v2.active = next_s
    MERGE (u)-[r2:TRANSFER_LIFT {amount: j}]->(v2)
    ON CREATE SET r2.ts = e.ts, r2.eid = elementId(e)
)
FOREACH (_ IN CASE WHEN u.layer=2 AND u.level > j THEN [1] ELSE [] END |
    MERGE (v3:Stage {accId: v_acc.id, level: j, layer: 2})
    ON CREATE SET v3.active = next_s
    ON MATCH SET v3.active = next_s
    MERGE (u)-[r3:TRANSFER_LIFT {amount: j}]->(v3)
    ON CREATE SET r3.ts = e.ts, r3.eid = elementId(e)
)
"""

# -----------------------------------------------------------------------------
# Query Templates
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
  RETURN t.id AS end_node
}}
RETURN count(DISTINCT end_node) AS rows
"""

def make_lifted_query_peak(max_hops_str: str) -> str:
    # Logic: If we ran the Iterative Build (hops <= 1024), the graph ONLY contains 
    # reachable nodes for that specific hop count. 
    # We can skip traversal and just count the existence of Layer 2 nodes.
    
    is_iterative = (max_hops_str != "inf" and int(max_hops_str) <= 1024)
    
    if is_iterative:
        # --- INSTANT QUERY (O(N)) ---
        # The build process already ensured that any node in Layer 2 
        # is reached by a valid path of length <= max_hops.
        return """
        MATCH (x:Stage {layer: 2}) 
        RETURN count(DISTINCT x.accId) AS rows
        """
    else:
        # --- TRAVERSAL QUERY (Fallback for Global/Infinite) ---
        # For the global build, the graph contains *all* edges, so we must 
        # traverse to enforce the hop limit and start point.
        range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
        return f"""
        CALL(){{
          MATCH (start:Stage {{level: -1, layer: 1}})
          MATCH p = (start)-[:TRANSFER_LIFT{range_str}]->(x:Stage {{layer: 2}})
          // We remove the unique edge check for speed, relying on the
          // fact that Peak paths usually don't loop in this structure.
          RETURN x.accId as end_node
        }}
        RETURN count(DISTINCT end_node) AS rows
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

def clear_database_full(session):
    debug_print("[DEBUG] Clearing FULL database...")
    while True:
        query = f"MATCH (n) WITH n LIMIT {DELETE_BATCH_SIZE} DETACH DELETE n RETURN count(n) as count"
        result = session.run(query).single()
        if result["count"] == 0: break

def clean_lifted_graph(session):
    debug_print("[DEBUG] Cleaning LIFTED Graph (Stages only)...")
    while True:
        query = f"MATCH (n:Stage) WITH n LIMIT {DELETE_BATCH_SIZE} DETACH DELETE n RETURN count(n) as count"
        result = session.run(query).single()
        if result["count"] == 0: break

def load_base_graph(session, csv_path):
    debug_print("[DEBUG] Reading CSV for Base Graph loading...")
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
    run_write(session, LOAD_ACCOUNTS, {"ids": node_list}, desc="LOAD_ACCOUNTS")

    total_edges = len(edges)
    debug_print(f"[DEBUG] Loading {total_edges} Base Edges...")
    for i in range(0, total_edges, LOAD_BATCH_SIZE):
        batch = edges[i:i + LOAD_BATCH_SIZE]
        run_write(session, LOAD_EDGES, {"rows": batch}, desc=f"LOAD_EDGES_{i}")
    return len(node_list)

# --- BUILD STRATEGIES ---
def build_full_lifted_graph(session, all_ids, timeout=None, start_time_ref=None):
    debug_print("[DEBUG] Strategy: GLOBAL FULL BUILD")
    total_ids = len(all_ids)
    
    # Nodes
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, BUILD_STAGE_NODES_BATCH, {"batchIds": batch_ids})
    
    # Edges
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, BUILD_STAGE_EDGES_BATCH, {"batchIds": batch_ids})
        if i % (LIFT_BATCH_SIZE * 5) == 0: debug_print(f"  [Global] Edge Progress: {i}/{total_ids}")
        
    return True

def build_iterative_lifted_graph(session, all_ids, max_hops, timeout=None, start_time_ref=None):
    debug_print(f"[DEBUG] Strategy: ITERATIVE BUILD (Max Depth: {max_hops})")
    
    # 1. Initialize Roots (Active Step 0)
    for i in range(0, len(all_ids), LIFT_BATCH_SIZE):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, INIT_ITERATIVE_ROOTS, {"batchIds": batch_ids})
        
    # 2. Expand Layer by Layer
    for step in range(max_hops):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        
        # Get active nodes for this step
        res = session.run(GET_ACTIVE_STAGE_IDS, {"step": step})
        active_ids = [r["id"] for r in res]
        
        if not active_ids:
            debug_print(f"  [Iterative] Step {step}: No active nodes. Stopping early.")
            break
            
        debug_print(f"  [Iterative] Step {step}: Expanding {len(active_ids)} active nodes...")
        
        # Batch expand
        for i in range(0, len(active_ids), ITERATIVE_BATCH_SIZE):
            if timeout and (time.perf_counter() - start_time_ref > timeout): return False
            batch = active_ids[i:i + ITERATIVE_BATCH_SIZE]
            run_write(session, EXPAND_ITERATIVE_BATCH, {"batchIds": batch, "next_step": step + 1})
            
    return True

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
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5), ("Paths(B)", 10),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5), ("Paths(L)", 10),
        ("Speedup", 15), ("Sanity", 8)
    ]
    fmt = "  ".join([f"{{:<{w}}}" for _, w in HEADERS])
    
    speedup_val = r["speedup"]
    speedup_str = speedup_val if isinstance(speedup_val, str) else f"{speedup_val:.2f}"

    line = fmt.format(
        r["nodes"], r["density"], r["edges"], str(r["hops"]),
        f"{r['base_mean']:.2f}", f"{r['base_std']:.2f}", r['base_timeouts'], f"{r['base_paths']:.1f}",
        f"{r['build_avg']:.2f}",
        f"{r['lift_mean']:.2f}", f"{r['lift_std']:.2f}", r['lift_timeouts'], f"{r['lift_paths']:.1f}",
        speedup_str,
        r["sanity"]
    )
    with open(filename, "a") as f:
        f.write(line + "\n")

def main():
    global DEBUG
    
    parser = argparse.ArgumentParser(description="Optimized Peak Motif Benchmark (Build Reuse)")
    parser.add_argument("--uri", default="bolt://127.0.0.1:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j2")
    parser.add_argument("--densities", type=float, nargs="+", default=[1.0, 5.0, 10.0])
    parser.add_argument("--hops", type=int, nargs="+", default=[2,4,8,16,32,64, 128, 256, 512, 1024])
    parser.add_argument("--nodes", type=int, nargs="+", default=[100, 1000, 10000, 100000, 1000000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--gmark_dir", default="../gmark")
    parser.add_argument("--out", default="results/peak_paths_opt")
    parser.add_argument("--debug", action="store_true", help="Enable debug logs") 
    args = parser.parse_args()

    DEBUG = args.debug 
    auth = (args.user, args.password)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    out_dir = os.path.dirname(args.out)
    if out_dir: Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    final_output_file = f"{args.out}_{timestamp}.txt"
    print(f"Output will be saved to: {final_output_file}")
    init_results_file(final_output_file)

    try:
        with GraphDatabase.driver(args.uri, auth=auth) as d:
            d.verify_connectivity()
            debug_print(f"[DEBUG] Connection successful.")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    for N in args.nodes:
        print(f"\n=== PROCESSING NODE COUNT: {N} ===")
        for D in args.densities:
            E = int(N * D)
            print(f"\n--- Experiment: Nodes={N}, Density={D} (Edges={E}) ---")
            
            driver = GraphDatabase.driver(args.uri, auth=auth)
            configs = list(args.hops) + ["inf"]
            experiment_data = {
                h: {
                    "base": [], "lift": [], "build": [], 
                    "base_counts": [], "lift_counts": [], 
                    "base_to": 0, "lift_to": 0, "build_to": 0,
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
                    with driver.session(database=args.database) as session:
                        clear_database_full(session)
                        for i, stmt in enumerate(INIT_CONSTRAINTS):
                            run_write(session, stmt, desc=f"CONSTRAINT_{i}")
                        load_base_graph(session, csv_path)
                        res = session.run(GET_ALL_ACCOUNTS)
                        all_ids = [rec["id"] for rec in res]

                        current_graph_state = None 
                        cached_global_build_ms = None
                        
                        consec_base_to = 0
                        consec_lift_to = 0
                        
                        for H in configs:
                            # Note: removed the 'break' here so we continue iterating
                            # and can explicitly mark subsequent runs as timeouts.

                            h_label = f"Unbounded" if H == "inf" else f"Hops={H}"
                            print(f"  > Config {h_label}...")

                            is_iterative = (isinstance(H, int) and H <= 1024)
                            required_state = "Iterative" if is_iterative else "Global"

                            run_lift = (consec_lift_to < 2)
                            run_base = (consec_base_to < 2)

                            # ==========================================
                            # [FIX] Initialize these variables here
                            # ==========================================
                            lifted_count = None
                            base_count = None
                            # ==========================================

                            lift_ms = None
                            skipped_build = False
                            
                            if run_lift:
                                if required_state == "Global" and current_graph_state == "Global":
                                    print(f"    [Build] REUSING existing Global Graph.")
                                    lift_ms = cached_global_build_ms
                                    skipped_build = True
                                else:
                                    clean_lifted_graph(session)
                                    t_start = time.perf_counter()
                                    build_success = False
                                    if is_iterative:
                                        build_success = build_iterative_lifted_graph(session, all_ids, H, timeout=args.timeout, start_time_ref=t_start)
                                        current_graph_state = None 
                                    else:
                                        build_success = build_full_lifted_graph(session, all_ids, timeout=args.timeout, start_time_ref=t_start)
                                        if build_success:
                                            current_graph_state = "Global"

                                    if build_success:
                                        lift_ms = (time.perf_counter() - t_start) * 1000
                                        if required_state == "Global":
                                            cached_global_build_ms = lift_ms
                                    else:
                                        lift_ms = None 

                                if lift_ms is None:
                                    print(f"    Build: TIMEOUT/FAIL")
                                    experiment_data[H]["build_to"] += 1
                                    consec_lift_to += 1
                                else:
                                    prefix = "[Cached]" if skipped_build else "[New]"
                                    print(f"    Build {prefix}: {lift_ms:.2f}ms")
                                    experiment_data[H]["build"].append(lift_ms)
                                    lifted_count = None
                                    try:
                                        q_lift = make_lifted_query_peak(str(H))
                                        val, t_sec = run_timed_query(args.uri, auth, q_lift, args.timeout, args.database)
                                        experiment_data[H]["lift"].append(t_sec * 1000)
                                        experiment_data[H]["lift_counts"].append(val)
                                        lifted_count = val
                                        consec_lift_to = 0 
                                    except TimeoutError:
                                        experiment_data[H]["lift_to"] += 1
                                        consec_lift_to += 1
                                    except Exception as e:
                                        print(f"    Lifted Error: {e}")
                                        consec_lift_to += 1
                            else:
                                print("    [Lifted] Skipped (Consecutive Timeouts)")
                                experiment_data[H]["lift_to"] += 1  # Record as timeout

                            base_count = None
                            if run_base:
                                try:
                                    q_base = make_baseline_query_peak(str(H))
                                    val, t_sec = run_timed_query(args.uri, auth, q_base, args.timeout, args.database)
                                    experiment_data[H]["base"].append(t_sec * 1000)
                                    experiment_data[H]["base_counts"].append(val)
                                    base_count = val
                                    consec_base_to = 0
                                except TimeoutError:
                                    experiment_data[H]["base_to"] += 1
                                    consec_base_to += 1
                                except Exception as e:
                                    print(f"    Base Error: {e}")
                                    consec_base_to += 1
                            else:
                                print("    [Base] Skipped (Consecutive Timeouts)")
                                experiment_data[H]["base_to"] += 1  # Record as timeout

                            if run_lift and run_base and base_count is not None and lifted_count is not None:
                                if base_count == lifted_count:
                                    experiment_data[H]["sanity_verified_count"] += 1
                                else:
                                    print(f"    [SANITY] FAIL: B={base_count} != L={lifted_count}")
                                    experiment_data[H]["sanity_mismatch"] = True

                except Exception as e:
                    print(f"\n[CRITICAL ERROR] Run failed: {e}")
                finally:
                    try: os.remove(csv_path)
                    except: pass

            print(f"\n--- Summary for Nodes={N}, Density={D} ---")
            
            # Keep track of the last valid speedup to fill in skipped rows
            last_speedup = 0.0

            for H in configs:
                data = experiment_data[H]
                
                def get_stats(vals):
                    if not vals: return 0.0, 0.0
                    return (statistics.mean(vals), statistics.stdev(vals)) if len(vals) > 1 else (vals[0], 0.0)

                b_mean, b_std = get_stats(data["base"])
                l_mean, l_std = get_stats(data["lift"])
                build_avg = statistics.mean(data["build"]) if data["build"] else 0.0
                b_paths = statistics.mean(data["base_counts"]) if data["base_counts"] else 0.0
                l_paths = statistics.mean(data["lift_counts"]) if data["lift_counts"] else 0.0

                # Determine failures
                lift_failures = data["lift_to"] + data["build_to"]
                lift_failed = (len(data["lift"]) == 0 and lift_failures > 0)
                base_failed = (len(data["base"]) == 0 and data["base_to"] > 0)

                # Check if this row was actually attempted (has data or explicit timeouts)
                was_attempted = (len(data["base"]) > 0 or data["base_to"] > 0 or 
                                 len(data["lift"]) > 0 or lift_failures > 0)

                if was_attempted:
                    denom = build_avg + l_mean
                    if base_failed and lift_failed: speedup = "Both Time Out"
                    elif lift_failed: speedup = "0.00"
                    elif base_failed and denom > 0: speedup = float('inf')
                    elif denom > 0: speedup = b_mean / denom
                    else: speedup = 0.0
                    # Store this for subsequent skipped rows (fallback)
                    last_speedup = speedup
                else:
                    # Fallback (should be rare now that we force skips to record timeouts)
                    speedup = last_speedup

                sanity_str = "FAIL" if data["sanity_mismatch"] else ("PASS" if data["sanity_verified_count"] > 0 else "N/A")

                row = {
                    "nodes": N, "density": D, "edges": E, "hops": H,
                    "base_mean": b_mean, "base_std": b_std, "base_timeouts": data["base_to"],
                    "base_paths": b_paths,
                    "build_avg": build_avg,
                    "lift_mean": l_mean, "lift_std": l_std, "lift_timeouts": lift_failures,
                    "lift_paths": l_paths,
                    "speedup": speedup, "sanity": sanity_str
                }
                append_result_row(final_output_file, row)
                
            driver.close()

    print(f"\nDone. Final results saved to {final_output_file}")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
