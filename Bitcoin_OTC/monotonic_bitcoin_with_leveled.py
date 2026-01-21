#!/usr/bin/env python3
import argparse
import csv
import sys
import time
import multiprocessing
import os
import statistics
import requests
import gzip
import io
from datetime import datetime
from pathlib import Path

# External Dependencies
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------

BITCOIN_OTC_URL = "https://snap.stanford.edu/data/soc-sign-bitcoinotc.csv.gz"

SETUP_TIMEOUT_SEC = 900.0 
LOAD_BATCH_SIZE = 50000      
LIFT_BATCH_SIZE = 5000       
DELETE_BATCH_SIZE = 10000    
ITERATIVE_BATCH_SIZE = 5000
START_LEVEL = -100

# -----------------------------------------------------------------------------
# Cypher Queries
# -----------------------------------------------------------------------------

INIT_CONSTRAINTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE",
    "CREATE INDEX stage_acc_level IF NOT EXISTS FOR (s:Stage) ON (s.accId, s.level)",
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

# --- GLOBAL BUILD ---
BUILD_STAGE_NODES_BATCH = f"""
UNWIND $batchIds AS aid
MATCH (v:Account {{id: aid}})
OPTIONAL MATCH (:Account)-[e:TRANSFER]->(v)
WITH v, collect(DISTINCT e.amount) AS inAmts
WITH v, [{START_LEVEL}] + [amt IN inAmts WHERE amt IS NOT NULL] AS levels
UNWIND levels AS level
MERGE (:Stage {{accId: v.id, level: level}})
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

# --- ITERATIVE BUILD ---
INIT_ITERATIVE_ROOTS = f"""
UNWIND $batchIds AS aid
MATCH (a:Account {{id: aid}})
MERGE (s:Stage {{accId: a.id, level: {START_LEVEL}}})
ON CREATE SET s.active = 0
ON MATCH SET s.active = 0
"""

GET_ACTIVE_STAGE_IDS = "MATCH (s:Stage) WHERE s.active = $step RETURN elementId(s) as id"

EXPAND_ITERATIVE_BATCH = """
UNWIND $batchIds AS sid
MATCH (u:Stage) WHERE elementId(u) = sid
MATCH (u_acc:Account {id: u.accId})-[e:TRANSFER]->(v_acc:Account)
WITH u, v_acc, e, e.amount as j, $next_step as next_s

FOREACH (_ IN CASE WHEN u.level < j THEN [1] ELSE [] END |
    MERGE (v1:Stage {accId: v_acc.id, level: j})
    ON CREATE SET v1.active = next_s
    ON MATCH SET v1.active = next_s
    MERGE (u)-[r1:TRANSFER_LIFT {amount: j}]->(v1)
    ON CREATE SET r1.ts = e.ts
)
"""

# -----------------------------------------------------------------------------
# Query Templates
# -----------------------------------------------------------------------------

def make_baseline_query(max_hops_str: str) -> str:
    range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
    return f"""
    MATCH (s:Account)
    CALL (s) {{
        MATCH p = (s)-[:TRANSFER{range_str}]->(t:Account)
        WITH s, t, [r IN relationships(p) | r.amount] AS amts
        WHERE size(amts) > 0 AND ALL(i IN range(1, size(amts)-1) WHERE amts[i-1] < amts[i])
        RETURN t
    }}
    WITH DISTINCT t
    RETURN count(t) AS rows
    """

def make_lifted_query_optimized(max_hops_str: str) -> str:
    is_iterative = (max_hops_str != "inf" and int(max_hops_str) <= 1024)
    if is_iterative:
        return f"""
        MATCH (x:Stage)
        WHERE x.active > 0
        RETURN count(DISTINCT x.accId) AS rows
        """
    else:
        range_str = f"*1..{max_hops_str}" if max_hops_str != "inf" else "*"
        return f"""
        MATCH (start:Stage {{level: {START_LEVEL}}})
        CALL (start) {{
          MATCH (start)-[:TRANSFER_LIFT{range_str}]->(x:Stage)
          RETURN count(DISTINCT x.accId) as reachable_targets
        }}
        RETURN sum(reachable_targets) AS rows
        """

# -----------------------------------------------------------------------------
# Data Loading
# -----------------------------------------------------------------------------

def download_and_parse_bitcoin(output_dir):
    print(f"[DEBUG] Downloading Bitcoin OTC dataset from: {BITCOIN_OTC_URL}")
    try:
        response = requests.get(BITCOIN_OTC_URL, stream=True)
        response.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Failed to download dataset: {e}")
        sys.exit(1)

    edges = []
    nodes = set()
    with gzip.open(io.BytesIO(response.content), 'rt') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3 or row[0].startswith('#'): continue
            try:
                u, v, rating = int(row[0]), int(row[1]), int(float(row[2]))
                edges.append({"src": u, "dst": v, "amount": rating})
                nodes.add(u); nodes.add(v)
            except ValueError: continue

    print(f"[DEBUG] Parse Complete. Found {len(nodes)} nodes and {len(edges)} edges.")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    csv_path = os.path.join(output_dir, "bitcoin_edges.csv")
    with open(csv_path, "w", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["src", "dst", "amount"])
        for e in edges: writer.writerow([e["src"], e["dst"], e["amount"]])
    return csv_path, len(nodes), len(edges)

# -----------------------------------------------------------------------------
# Neo4j Operations
# -----------------------------------------------------------------------------

def run_write(session, cypher, params=None, desc="Query"):
    def work(tx): tx.run(cypher, params or {}, timeout=SETUP_TIMEOUT_SEC).consume()
    session.execute_write(work)

def clear_database_full(session):
    print("[DEBUG] Clearing FULL database...")
    while True:
        query = f"MATCH (n) WITH n LIMIT {DELETE_BATCH_SIZE} DETACH DELETE n RETURN count(n) as count"
        if session.run(query).single()["count"] == 0: break

def clean_lifted_graph(session):
    print("[DEBUG] Cleaning LIFTED Graph (Stages only)...")
    while True:
        query = f"MATCH (n:Stage) WITH n LIMIT {DELETE_BATCH_SIZE} DETACH DELETE n RETURN count(n) as count"
        if session.run(query).single()["count"] == 0: break

def load_graph_to_neo4j(session, csv_path):
    print("[DEBUG] Reading CSV for Neo4j loading...")
    edges = []
    nodes = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            edges.append({"src": int(row["src"]), "dst": int(row["dst"]), "label": int(row["amount"])})
            nodes.add(int(row["src"])); nodes.add(int(row["dst"]))
    
    node_list = sorted(list(nodes))
    for i in range(0, len(node_list), LOAD_BATCH_SIZE):
        run_write(session, LOAD_ACCOUNTS, {"ids": node_list[i:i + LOAD_BATCH_SIZE]})

    for i in range(0, len(edges), LOAD_BATCH_SIZE):
        run_write(session, LOAD_EDGES, {"rows": edges[i:i + LOAD_BATCH_SIZE]})

# --- BUILD FUNCTIONS ---

def build_full_lifted_graph(session, all_ids, timeout=None, start_time_ref=None):
    print("[DEBUG] Strategy: GLOBAL FULL BUILD")
    for i in range(0, len(all_ids), LIFT_BATCH_SIZE):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        run_write(session, BUILD_STAGE_NODES_BATCH, {"batchIds": all_ids[i:i + LIFT_BATCH_SIZE]})
    for i in range(0, len(all_ids), LIFT_BATCH_SIZE):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        run_write(session, BUILD_STAGE_EDGES_BATCH, {"batchIds": all_ids[i:i + LIFT_BATCH_SIZE]})
    return True

def build_iterative_lifted_graph(session, all_ids, max_hops, timeout=None, start_time_ref=None):
    print(f"[DEBUG] Strategy: ITERATIVE BUILD (Max Depth: {max_hops})")
    for i in range(0, len(all_ids), LIFT_BATCH_SIZE):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        run_write(session, INIT_ITERATIVE_ROOTS, {"batchIds": all_ids[i:i + LIFT_BATCH_SIZE]})
        
    for step in range(max_hops):
        if timeout and (time.perf_counter() - start_time_ref > timeout): return False
        active_ids = [r["id"] for r in session.run(GET_ACTIVE_STAGE_IDS, {"step": step})]
        if not active_ids: break
        print(f"  [Iterative] Step {step}: Expanding {len(active_ids)} active nodes...")
        for i in range(0, len(active_ids), ITERATIVE_BATCH_SIZE):
            if timeout and (time.perf_counter() - start_time_ref > timeout): return False
            run_write(session, EXPAND_ITERATIVE_BATCH, {"batchIds": active_ids[i:i + ITERATIVE_BATCH_SIZE], "next_step": step + 1})
    return True

# -----------------------------------------------------------------------------
# Main & Workers
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

def write_results_to_file(filename, results_dict, hop_list):
    # This is called AT THE END to ensure headers are written and file is clean
    headers = [
        ("Type", 12), ("Hops", 6),
        ("B.Mean(ms)", 12), ("B.TO", 6),
        ("Build(ms)", 12),
        ("L.Mean(ms)", 12), ("L.TO", 6),
        ("Speedup", 12), ("Sanity", 8)
    ]
    
    with open(filename, "w") as f:
        # Write Header
        header_str = "  ".join([f"{h[0]:<{h[1]}}" for h in headers])
        f.write(header_str + "\n")
        f.write("-" * len(header_str) + "\n")
        
        # Write Rows
        for H in hop_list:
            data = results_dict[H]
            b_mean = statistics.mean(data["base"]) if data["base"] else 0.0
            l_mean = statistics.mean(data["lift"]) if data["lift"] else 0.0
            build_avg = statistics.mean(data["build"]) if data["build"] else 0.0
            
            lift_fail = (len(data["lift"]) == 0 and (data["lift_to"] > 0 or data["build_to"] > 0))
            # Skipped counts as a failure for speedup calculation purposes
            base_fail = (len(data["base"]) == 0 and (data["base_to"] > 0 or data["skipped"]))
            
            denom = build_avg + l_mean
            
            if base_fail and lift_fail: speedup = "Both TO"
            elif lift_fail: speedup = 0.0
            elif base_fail and denom > 0: speedup = float('inf')
            elif denom > 0: speedup = b_mean / denom
            else: speedup = 0.0
            
            if isinstance(speedup, (int, float)) and speedup != float('inf'):
                sp_str = f"{speedup:.2f}x"
            elif speedup == float('inf'):
                sp_str = "Inf"
            else:
                sp_str = str(speedup)

            sanity = "PASS" if data["sanity_verified_count"] > 0 and not data["sanity_mismatch"] else ("FAIL" if data["sanity_mismatch"] else "N/A")
            type_label = "Unbounded" if H == "inf" else "Bounded"
            hops_str = str(H)
            
            line = (
                f"{type_label:<12}  {hops_str:<6}  "
                f"{b_mean:<12.2f}  {data['base_to']:<6}  "
                f"{build_avg:<12.2f}  "
                f"{l_mean:<12.2f}  {data['lift_to']:<6}  "
                f"{sp_str:<12}  {sanity:<8}"
            )
            f.write(line + "\n")
    print(f"Results successfully written to {filename}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="bolt://127.0.0.1:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="neo4j2")
    parser.add_argument("--hops", type=int, nargs="+", default=[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]) 
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--out_dir", default="results")
    args = parser.parse_args()

    auth = (args.user, args.password)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    final_output_file = os.path.join(args.out_dir, f"bitcoin_results_{timestamp}.txt")
    
    print(f"Results: {final_output_file}")
    csv_path, _, _ = download_and_parse_bitcoin("/tmp/bitcoin_fix_exp")

    configs = list(args.hops) + ["inf"]
    # Added "skipped": False to tracking
    experiment_data = {h: {"base": [], "lift": [], "build": [], "base_counts": [], "lift_counts": [], "base_to": 0, "lift_to": 0, "build_to": 0, "sanity_mismatch": False, "sanity_verified_count": 0, "skipped": False} for h in configs}

    try:
        driver = GraphDatabase.driver(args.uri, auth=auth)
        driver.verify_connectivity()
        
        for r in range(args.repeats):
            print(f"\n=== Run {r+1}/{args.repeats} ===")
            with driver.session(database=args.database) as session:
                clear_database_full(session)
                for i, stmt in enumerate(INIT_CONSTRAINTS): run_write(session, stmt)
                load_graph_to_neo4j(session, csv_path)
                res = session.run(GET_ALL_ACCOUNTS)
                all_ids = [rec["id"] for rec in res]

                current_graph_state = None; cached_global_build_ms = None
                consec_base_to = 0; consec_lift_to = 0

                for H in configs:
                    h_label = f"Unbounded" if H == "inf" else f"Hops={H}"
                    print(f"  > Config {h_label}...")

                    is_iterative = (isinstance(H, int) and H <= 1024)
                    required_state = "Iterative" if is_iterative else "Global"
                    run_lift = (consec_lift_to < 2); run_base = (consec_base_to < 2)
                    lifted_count = None; base_count = None; lift_ms = None

                    # --- LIFTED ---
                    if run_lift:
                        if required_state == "Global" and current_graph_state == "Global":
                            lift_ms = cached_global_build_ms
                        else:
                            clean_lifted_graph(session)
                            t_start = time.perf_counter()
                            if is_iterative:
                                success = build_iterative_lifted_graph(session, all_ids, H, args.timeout, t_start)
                                current_graph_state = None
                            else:
                                success = build_full_lifted_graph(session, all_ids, args.timeout, t_start)
                                if success: current_graph_state = "Global"
                            lift_ms = (time.perf_counter() - t_start) * 1000 if success else None
                            if required_state == "Global" and success: cached_global_build_ms = lift_ms

                        if lift_ms is None:
                            print(f"    Build: TIMEOUT"); experiment_data[H]["build_to"] += 1; consec_lift_to += 1
                        else:
                            experiment_data[H]["build"].append(lift_ms)
                            try:
                                val, t_sec = run_timed_query(args.uri, auth, make_lifted_query_optimized(str(H)), args.timeout, args.database)
                                experiment_data[H]["lift"].append(t_sec * 1000)
                                experiment_data[H]["lift_counts"].append(val)
                                lifted_count = val; print(f"    Lift: {t_sec*1000:.2f}ms ({val})"); consec_lift_to = 0
                            except TimeoutError:
                                print(f"    Lift: TIMEOUT"); experiment_data[H]["lift_to"] += 1; consec_lift_to += 1
                    else:
                        print("    [Lift] Skipped")

                    # --- BASELINE ---
                    if run_base:
                        try:
                            val, t_sec = run_timed_query(args.uri, auth, make_baseline_query(str(H)), args.timeout, args.database)
                            experiment_data[H]["base"].append(t_sec * 1000)
                            experiment_data[H]["base_counts"].append(val)
                            base_count = val; print(f"    Base: {t_sec*1000:.2f}ms ({val})"); consec_base_to = 0
                        except TimeoutError:
                            print(f"    Base: TIMEOUT"); experiment_data[H]["base_to"] += 1; consec_base_to += 1
                    else:
                        print("    [Base] Skipped")
                        # Explicitly mark as skipped and force timeout count to repeats so stats reflect it
                        experiment_data[H]["skipped"] = True
                        experiment_data[H]["base_to"] = args.repeats

                    if lifted_count is not None and base_count is not None:
                        if lifted_count == base_count: experiment_data[H]["sanity_verified_count"] += 1
                        else:
                            experiment_data[H]["sanity_mismatch"] = True
                            print(f"    [FAIL] Mismatch: B={base_count}, L={lifted_count}")

    except Exception as e: print(f"Error: {e}")
    finally: 
        driver.close()
        # WRITE FILE AT THE VERY END
        write_results_to_file(final_output_file, experiment_data, configs)

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
