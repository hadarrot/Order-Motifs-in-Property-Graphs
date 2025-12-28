# Order Motifs in Property Graphs (Source Code)

Code + scripts to reproduce the experiments from **Compiling Order Motifs into Reachability** paper.  
We evaluate compiling order-aware path constraints into plain **reachability** using a materialized *motif-leveled* graph.

> ⚠️ The scripts **delete all data** in the selected Neo4j database. Use a dedicated empty DB.

## Repo structure
- `exp1/bounded_paths.py` — Exp 1 (bounded increasing paths)
- `exp2/unbounded_paths.py` — Exp 2 (unbounded increasing paths)
- `exp3/peak_paths.py` — Exp 3 (peak motif)
- `exp4/motif_scaling.py` — Exp 4 (scaling with motif length / wildcards)

## Requirements
- Neo4j reachable via Bolt
- Python 3.10+
- gMark binary at <GMARK_DIR>/src/test (default --gmark_dir ../gmark)

