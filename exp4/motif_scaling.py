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

# python3 motif_scaling.py --password XXXX --nodes 10000 --debug

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

SETUP_TIMEOUT_SEC = 1800.0
LOAD_BATCH_SIZE = 50000    
LIFT_BATCH_SIZE = 1000     
DELETE_BATCH_SIZE = 10000  
ITERATIVE_BATCH_SIZE = 2000

# -----------------------------------------------------------------------------
# Dynamic Cypher Generators
# -----------------------------------------------------------------------------

def get_operator(char_code):
    if char_code == 'U': return "<", "UP"
    if char_code == 'D': return ">", "DOWN"
    raise ValueError(f"Unknown motif char: {char_code}")

# --- GLOBAL BUILD QUERIES (Legacy/Unbounded) ---
def generate_build_nodes_query(motif_len):
    layers = list(range(1, motif_len + 1))
    return f"""
UNWIND $batchIds AS aid
MATCH (v:Account {{id: aid}})
OPTIONAL MATCH (:Account)-[e:TRANSFER]->(v)
WITH v, collect(DISTINCT e.amount) AS inAmts
WITH v, [-1] + [amt IN inAmts WHERE amt IS NOT NULL] AS levels
UNWIND levels AS level
UNWIND {layers} AS layer
MERGE (:Stage {{accId: v.id, level: level, layer: layer}})
"""

def generate_build_edges_queries_list(motif_str):
    queries = []
    preamble = """
UNWIND $batchIds AS aid
MATCH (u:Account {id: aid})-[e:TRANSFER]->(v:Account)
WITH u, v, e, e.amount AS j, e.ts AS ts
"""
    for i, char in enumerate(motif_str):
        layer = i + 1
        op, name = get_operator(char)
        
        # Intra-Motif Logic
        intra_pred = f"s_u.level {op} j"
        q_intra = preamble + f"""
        MATCH (s_u:Stage {{accId: u.id, layer: {layer}}})
        WHERE {intra_pred}
        MATCH (s_v:Stage {{accId: v.id, level: j, layer: {layer}}})
        MERGE (s_u)-[r:TRANSFER_LIFT]->(s_v)
          ON CREATE SET r.amount = j, r.ts = ts
        """
        queries.append(q_intra)

        # Switching Logic
        if layer < len(motif_str):
            next_op, _ = get_operator(motif_str[layer]) 
            next_layer = layer + 1
            switch_pred = f"s_u.level {next_op} j"
            
            q_switch = preamble + f"""
            MATCH (s_u:Stage {{accId: u.id, layer: {layer}}})
            WHERE s_u.level <> -1 AND {switch_pred} 
            MATCH (s_v:Stage {{accId: v.id, level: j, layer: {next_layer}}})
            MERGE (s_u)-[r:TRANSFER_LIFT]->(s_v)
              ON CREATE SET r.amount = j, r.ts = ts
            """
            queries.append(q_switch)
    return queries

# --- ITERATIVE BUILD QUERIES (Leveled Optimization) ---
INIT_ITERATIVE_ROOTS = """
UNWIND $batchIds AS aid
MATCH (a:Account {id: aid})
MERGE (s:Stage {accId: a.id, level: -1, layer: 1})
ON CREATE SET s.active = 0
ON MATCH SET s.active = 0
"""

GET_ACTIVE_STAGE_IDS = "MATCH (s:Stage) WHERE s.active = $step RETURN elementId(s) as id"

def generate_iterative_expansion_query(motif_str):
    """
    Standard Iterative Expansion.
    """
    query = """
UNWIND $batchIds AS sid
MATCH (u:Stage) WHERE elementId(u) = sid
MATCH (u_acc:Account {id: u.accId})-[e:TRANSFER]->(v_acc:Account)
WITH u, v_acc, e, e.amount as j, $next_step as next_s
"""
    
    for i, char in enumerate(motif_str):
        layer = i + 1
        op, _ = get_operator(char)
        
        # 1. Intra-Layer Expansion
        pred_intra = f"u.layer = {layer} AND u.level {op} j"
        
        query += f"""
FOREACH (_ IN CASE WHEN {pred_intra} THEN [1] ELSE [] END |
    MERGE (v{layer}:Stage {{accId: v_acc.id, level: j, layer: {layer}}})
    ON CREATE SET v{layer}.active = next_s
    ON MATCH SET v{layer}.active = next_s
    MERGE (u)-[r{layer}:TRANSFER_LIFT]->(v{layer})
    ON CREATE SET r{layer}.amount = j, r{layer}.ts = e.ts
)
"""
        # 2. Switching Expansion
        if layer < len(motif_str):
            next_char = motif_str[layer] 
            next_op, _ = get_operator(next_char)
            next_layer = layer + 1
            pred_switch = f"u.layer = {layer} AND u.level <> -1 AND u.level {next_op} j"
            
            query += f"""
FOREACH (_ IN CASE WHEN {pred_switch} THEN [1] ELSE [] END |
    MERGE (v{next_layer}_sw:Stage {{accId: v_acc.id, level: j, layer: {next_layer}}})
    ON CREATE SET v{next_layer}_sw.active = next_s
    ON MATCH SET v{next_layer}_sw.active = next_s
    MERGE (u)-[r{next_layer}_sw:TRANSFER_LIFT]->(v{next_layer}_sw)
    ON CREATE SET r{next_layer}_sw.amount = j, r{next_layer}_sw.ts = e.ts
)
"""
    return query

# --- QUERIES ---

def generate_native_query(motif_str, max_hops_str):
    """
    Baseline Query: Counts REACHABLE END NODES (DISTINCT t.id).
    """
    range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
    
    def gen_segment_pred(start_idx, end_idx, char_code):
        op, _ = get_operator(char_code)
        return f"ALL(x IN range({start_idx}+1, {end_idx}-1) WHERE amts[x-1] {op} amts[x])"

    def build_nested_any(current_stage, start_var, total_stages):
        char_code = motif_str[current_stage]
        is_last = (current_stage == total_stages - 1)
        remaining_stages = total_stages - 1 - current_stage
        
        end_var = "size(amts)" if is_last else f"i_{current_stage}"
        seg_pred = gen_segment_pred(start_var, end_var, char_code)
        
        if is_last:
            return seg_pred
        
        next_char = motif_str[current_stage + 1]
        op_next, _ = get_operator(next_char)
        trans_pred = f"amts[{end_var}-1] {op_next} amts[{end_var}]"
        
        return f"""ANY({end_var} IN range({start_var}+1, size(amts)-{remaining_stages}) 
                   WHERE {seg_pred} 
                   AND {trans_pred}
                   AND {build_nested_any(current_stage + 1, end_var, total_stages)})"""

    return f"""
CALL(){{
  MATCH p = (s:Account)-[:TRANSFER{range_str}]->(t:Account)
  WITH p, t, [r IN relationships(p) | r.amount] AS amts
  WHERE size(amts) >= {len(motif_str)} AND {build_nested_any(0, "0", len(motif_str))}
  RETURN DISTINCT t.id as tid
}}
RETURN count(tid) AS rows
"""

def make_lifted_query_final(motif_len, max_hops_str):
    """
    Lifted Query: Instant Node Count (DISTINCT ACCOUNTS).
    """
    is_iterative = (max_hops_str != "inf" and int(max_hops_str) <= 1024)
    
    if is_iterative:
        # Optimization: We built only reachable nodes. 
        # But we must count DISTINCT accId to match Base Query semantics.
        return f"""
        MATCH (x:Stage {{layer: {motif_len}}}) 
        RETURN count(DISTINCT x.accId) AS rows
        """
    else:
        # Fallback for global
        range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
        return f"""
        CALL(){{
          MATCH (start:Stage {{level: -1, layer: 1}})
          MATCH p = (start)-[:TRANSFER_LIFT{range_str}]->(x:Stage {{layer: {motif_len}}})
          RETURN DISTINCT x.accId as tid
        }}
        RETURN count(tid) AS rows
        """

# -----------------------------------------------------------------------------
# Infrastructure & Utils
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

def run_shell(cmd, cwd=None):
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stderr}")

def update_gmark_schema(schema_path, n_edges, n_nodes):
    edges_per_node = n_edges / n_nodes if n_nodes > 0 else 1
    val = max(1, int(round(edges_per_node))) if edges_per_node >= 0.5 else 1
    xml_content = f'''<generator><graph><nodes>0</nodes></graph>
  <predicates><size>{n_edges}</size><alias symbol="0">transfer</alias></predicates>
  <types><size>1</size><alias type="0">Account</alias></types>
  <schema><source type="0"><target type="0" symbol="0" multiplicity="*">
  <indistribution type="uniform"><min>{val}</min><max>{val}</max></indistribution>
  <outdistribution type="uniform"><min>{val}</min><max>{val}</max></outdistribution>
  </target></source></schema><workload id="0" size="0"/></generator>'''
    with open(schema_path, "w") as f: f.write(xml_content)

def generate_gmark_csv(n_nodes, n_edges, gmark_dir, output_dir, schema_file="shop.xml"):
    debug_print(f"[DEBUG] Generating gMark graph: {n_nodes} nodes, {n_edges} edges...")
    src_dir = os.path.join(gmark_dir, "src")
    schema_path = os.path.join(gmark_dir, f"use-cases/{schema_file}")
    update_gmark_schema(schema_path, n_edges, n_nodes)
    run_shell(["./test", "-c", f"../use-cases/{schema_file}", "-g", "../demo/play/play-graph.txt",
        "-w", "../demo/play/play-workload.xml", "-r", "../demo/play", "-n", str(n_nodes)], cwd=src_dir)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_path = os.path.join(output_dir, "edges.csv")
    with open(csv_path, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["src", "dst", "amount"])
        if (n_edges / n_nodes) < 0.5:
            nodes = list(range(n_nodes))
            count = 0
            while count < n_edges:
                writer.writerow([random.choice(nodes), random.choice(nodes), random.randint(1, 1000)])
                count += 1
        else:
            files = sorted(glob.glob(os.path.join(src_dir, "../demo/play/play-graph.txt*")))
            for gf in files:
                with open(gf, "r") as fin:
                    for line in fin:
                        parts = line.strip().split()
                        if len(parts) >= 3: writer.writerow([parts[0], parts[2], random.randint(1, 1000)])
    return csv_path

def run_write(session, cypher, params=None, desc="Query"):
    def work(tx): tx.run(cypher, params or {}, timeout=SETUP_TIMEOUT_SEC).consume()
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
            nodes.add(u); nodes.add(v)
    node_list = sorted(list(nodes))
    run_write(session, LOAD_ACCOUNTS, {"ids": node_list})
    for i in range(0, len(edges), LOAD_BATCH_SIZE):
        batch = edges[i:i + LOAD_BATCH_SIZE]
        run_write(session, LOAD_EDGES, {"rows": batch})

def build_full_lifted_graph(session, all_ids, motif_str, timeout_sec, start_time_ref):
    debug_print(f"[DEBUG] Strategy: GLOBAL FULL BUILD")
    total_ids = len(all_ids)
    q_nodes = generate_build_nodes_query(len(motif_str))
    edge_queries = generate_build_edges_queries_list(motif_str)
    for i in range(0, total_ids, LIFT_BATCH_SIZE):
        if (time.perf_counter() - start_time_ref) > timeout_sec: return False
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, q_nodes, {"batchIds": batch_ids})
    for q_edge in edge_queries:
        for i in range(0, total_ids, LIFT_BATCH_SIZE):
            if (time.perf_counter() - start_time_ref) > timeout_sec: return False
            batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
            run_write(session, q_edge, {"batchIds": batch_ids})
    return True

def build_iterative_lifted_graph(session, all_ids, motif_str, max_hops, timeout_sec, start_time_ref):
    debug_print(f"[DEBUG] Strategy: ITERATIVE BUILD (Max Depth: {max_hops})")
    for i in range(0, len(all_ids), LIFT_BATCH_SIZE):
        if (time.perf_counter() - start_time_ref) > timeout_sec: return False
        batch_ids = all_ids[i:i + LIFT_BATCH_SIZE]
        run_write(session, INIT_ITERATIVE_ROOTS, {"batchIds": batch_ids})
    expand_query = generate_iterative_expansion_query(motif_str)
    for step in range(max_hops):
        if (time.perf_counter() - start_time_ref) > timeout_sec: return False
        res = session.run(GET_ACTIVE_STAGE_IDS, {"step": step})
        active_ids = [r["id"] for r in res]
        if not active_ids: break
        debug_print(f"  [Iterative] Step {step}: Expanding {len(active_ids)} active nodes...")
        for i in range(0, len(active_ids), ITERATIVE_BATCH_SIZE):
            if (time.perf_counter() - start_time_ref) > timeout_sec: return False
            batch = active_ids[i:i + ITERATIVE_BATCH_SIZE]
            run_write(session, expand_query, {"batchIds": batch, "next_step": step + 1})
    return True

# -----------------------------------------------------------------------------
# Main Execution
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
        if status == "ok": return data
        raise Neo4jError("ClientError", data)
    else:
        p.terminate(); p.join()
        raise TimeoutError(f"Query exceeded {timeout}s")

def init_results_file(filename):
    HEADERS = [
        ("Motif", 10), ("Nodes", 8), ("Density", 7), ("Hops", 5),
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5), ("Nodes(B)", 10),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5), ("Nodes(L)", 10),
        ("Speedup", 15), ("Sanity", 8)
    ]
    fmt = "  ".join([f"{{:<{w}}}" for _, w in HEADERS])
    with open(filename, "w") as f:
        f.write(fmt.format(*[h[0] for h in HEADERS]) + "\n")
        f.write("  ".join(["-" * w for _, w in HEADERS]) + "\n")

def append_result_row(filename, r):
    HEADERS = [
        ("Motif", 10), ("Nodes", 8), ("Density", 7), ("Hops", 5),
        ("B.Mean(ms)", 10), ("B.Std(ms)", 10), ("B.TO", 5), ("Nodes(B)", 10),
        ("Build(ms)", 10), ("L.Mean(ms)", 10), ("L.Std(ms)", 10), ("L.TO", 5), ("Nodes(L)", 10),
        ("Speedup", 15), ("Sanity", 8)
    ]
    fmt = "  ".join([f"{{:<{w}}}" for _, w in HEADERS])
    s_val = r["speedup"]
    s_str = s_val if isinstance(s_val, str) else f"{s_val:.2f}"
    line = fmt.format(
        r["motif"], r["nodes"], r["density"], str(r["hops"]),
        f"{r['base_mean']:.2f}", f"{r['base_std']:.2f}", r['base_timeouts'], f"{r['base_paths']:.1f}",
        f"{r['build_avg']:.2f}",
        f"{r['lift_mean']:.2f}", f"{r['lift_std']:.2f}", r['lift_timeouts'], f"{r['lift_paths']:.1f}",
        s_str, r["sanity"]
    )
    with open(filename, "a") as f: f.write(line + "\n")
    print(line)

def main():
    global DEBUG
    parser = argparse.ArgumentParser(description="Motif Scaling (Iterative Leveled)")
    parser.add_argument("--uri", default="bolt://127.0.0.1:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--densities", type=float, nargs="+", default=[1.0, 5.0, 10.0])
    parser.add_argument("--nodes", type=int, nargs="+", default=[10000])
    parser.add_argument("--hops", type=int, nargs="+", default=[2,4,8,16,32,64,128,256,512,1024])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--gmark_dir", default="../gmark")
    parser.add_argument("--out", default="results/motif_scaling")
    parser.add_argument("--debug", action="store_true") 
    args = parser.parse_args()

    DEBUG = args.debug 
    auth = (args.user, args.password)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    out_dir = os.path.dirname(args.out)
    if out_dir: Path(out_dir).mkdir(parents=True, exist_ok=True)
    final_output_file = f"{args.out}_zigzag_{timestamp}.txt"
    print(f"Output: {final_output_file}")
    init_results_file(final_output_file)

    try:
        with GraphDatabase.driver(args.uri, auth=auth) as d: d.verify_connectivity()
    except Exception as e:
        print(f"Connection failed: {e}"); sys.exit(1)

    motifs_to_test = []
    base = ['U', 'D']
    for k in [2, 3, 4, 5]:
        m = (base * 3)[:k]
        motifs_to_test.append("".join(m))

    for N in args.nodes:
        print(f"\n=== PROCESSING NODE COUNT: {N} ===")
        for D in args.densities:
            E = int(N * D)
            print(f"\n--- Density={D} (Edges={E}) ---")

            for motif in motifs_to_test:
                print(f"\n>>> Testing Motif: {motif} (Len: {len(motif)})")
                configs = list(args.hops) + ["inf"]
                experiment_data = {
                    h: {
                        "base": [], "lift": [], "build": [], 
                        "base_counts": [], "lift_counts": [], 
                        "base_to": 0, "lift_to": 0, "build_to": 0,
                        "sanity_mismatch": False, "sanity_verified_count": 0
                    } for h in configs
                }
                for r in range(args.repeats):
                    debug_print(f"\n[DEBUG] --- Run {r+1}/{args.repeats} ---")
                    tmp_dir = f"/tmp/exp_run_{r}"
                    csv_path = generate_gmark_csv(N, E, args.gmark_dir, tmp_dir)
                    try:
                        driver = GraphDatabase.driver(args.uri, auth=auth)
                        with driver.session(database=args.database) as session:
                            clear_database_full(session)
                            for i, stmt in enumerate(INIT_CONSTRAINTS): run_write(session, stmt)
                            load_base_graph(session, csv_path)
                            res = session.run(GET_ALL_ACCOUNTS)
                            all_ids = [rec["id"] for rec in res]

                            current_graph_state = None
                            cached_global_build_ms = None
                            consec_base_to = 0
                            consec_lift_to = 0

                            for H in configs:
                                h_label = f"Unbounded" if H == "inf" else f"Hops={H}"
                                print(f"  > Config {h_label}...")
                                is_iterative = (isinstance(H, int) and H <= 1024)
                                required_state = "Iterative" if is_iterative else "Global"
                                run_lift = (consec_lift_to < 2)
                                run_base = (consec_base_to < 2)
                                lift_ms = None; lifted_count = None; base_count = None; skipped_build = False

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
                                            build_success = build_iterative_lifted_graph(session, all_ids, motif, H, args.timeout, t_start)
                                            current_graph_state = None 
                                        else:
                                            build_success = build_full_lifted_graph(session, all_ids, motif, args.timeout, t_start)
                                            if build_success: current_graph_state = "Global"
                                        if build_success:
                                            lift_ms = (time.perf_counter() - t_start) * 1000
                                            if required_state == "Global": cached_global_build_ms = lift_ms
                                        else: lift_ms = None 

                                    if lift_ms is None:
                                        print(f"    Build: TIMEOUT/FAIL")
                                        experiment_data[H]["build_to"] += 1
                                        consec_lift_to += 1
                                    else:
                                        prefix = "[Cached]" if skipped_build else "[New]"
                                        print(f"    Build {prefix}: {lift_ms:.2f}ms")
                                        experiment_data[H]["build"].append(lift_ms)
                                        q_lift = make_lifted_query_final(len(motif), str(H))
                                        try:
                                            val, t_sec = run_timed_query(args.uri, auth, q_lift, args.timeout, args.database)
                                            experiment_data[H]["lift"].append(t_sec * 1000)
                                            experiment_data[H]["lift_counts"].append(val)
                                            lifted_count = val
                                            consec_lift_to = 0
                                        except TimeoutError:
                                            experiment_data[H]["lift_to"] += 1
                                            consec_lift_to += 1
                                        except Exception as e:
                                            print(f"    Lift Query Error: {e}")
                                            consec_lift_to += 1
                                else: experiment_data[H]["lift_to"] += 1

                                if run_base:
                                    q_base = generate_native_query(motif, str(H))
                                    try:
                                        val, t_sec = run_timed_query(args.uri, auth, q_base, args.timeout, args.database)
                                        experiment_data[H]["base"].append(t_sec * 1000)
                                        experiment_data[H]["base_counts"].append(val)
                                        base_count = val
                                        consec_base_to = 0
                                    except TimeoutError:
                                        experiment_data[H]["base_to"] += 1
                                        consec_base_to += 1
                                    except Exception as e:
                                        print(f"    Base Query Error: {e}")
                                        consec_base_to += 1
                                else: experiment_data[H]["base_to"] += 1

                                if base_count is not None and lifted_count is not None:
                                    if base_count == lifted_count: experiment_data[H]["sanity_verified_count"] += 1
                                    else:
                                        experiment_data[H]["sanity_mismatch"] = True
                                        print(f"    [SANITY] FAIL: Base={base_count} vs Lift={lifted_count}")

                    except Exception as e: print(f"Run Error: {e}")
                    finally:
                        try: os.remove(csv_path)
                        except: pass

                print(f"--- Summary for Motif={motif} ---")
                last_speedup = 0.0
                for H in configs:
                    data = experiment_data[H]
                    def get_stats(vals):
                        if not vals: return 0.0, 0.0
                        return statistics.mean(vals), statistics.stdev(vals) if len(vals)>1 else 0.0
                    b_mean, b_std = get_stats(data["base"])
                    l_mean, l_std = get_stats(data["lift"])
                    build_avg = statistics.mean(data["build"]) if data["build"] else 0.0
                    base_paths = statistics.mean(data["base_counts"]) if data["base_counts"] else 0.0
                    lift_paths = statistics.mean(data["lift_counts"]) if data["lift_counts"] else 0.0
                    lift_failures = data["lift_to"] + data["build_to"]
                    lift_failed = (len(data["lift"]) == 0 and lift_failures > 0)
                    base_failed = (len(data["base"]) == 0 and data["base_to"] > 0)
                    was_attempted = (len(data["base"]) > 0 or data["base_to"] > 0 or len(data["lift"]) > 0 or lift_failures > 0)

                    if was_attempted:
                        denom = build_avg + l_mean
                        if base_failed and lift_failed: speedup = "Both Time Out"
                        elif lift_failed: speedup = "0.00"
                        elif base_failed and denom > 0: speedup = float('inf')
                        elif denom > 0: speedup = b_mean / denom
                        else: speedup = 0.0
                        last_speedup = speedup
                    else: speedup = last_speedup
                    sanity = "FAIL" if data["sanity_mismatch"] else ("PASS" if data["sanity_verified_count"] > 0 else "N/A")
                    row = {
                        "motif": motif, "nodes": N, "density": D, "hops": H,
                        "base_mean": b_mean, "base_std": b_std, "base_timeouts": data["base_to"], 
                        "base_paths": base_paths,
                        "build_avg": build_avg,
                        "lift_mean": l_mean, "lift_std": l_std, "lift_timeouts": lift_failures,
                        "lift_paths": lift_paths,
                        "speedup": speedup, "sanity": sanity
                    }
                    append_result_row(final_output_file, row)

    print(f"\nDone. Results: {final_output_file}")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
