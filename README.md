# PageRank 2025-2026 — TP "Large Scale Data Management" (Pascal Molli)

**Sujet / objectif**
Comparer les performances de PageRank implémenté en **PySpark RDD** vs **PySpark DataFrame** sur le jeu de données Wikilinks (DBpedia).

**Auteurs :**
- Maxime POULALION

## Résumé du protocole expérimental
- Implémentations : `pagerank_rdd.py` (RDD) et `pagerank_df.py` (DataFrame).
- Jeux de données : 1% (local, tests), 10% (cluster tests).
- Configs : 2/4/6 nœuds (≤ 32 vCPU total).
- Fixer `num_iters=10` pour comparaisons.

## Quick start (sur Debian/Ubuntu)
1. Installer Java, Python, Spark (ex: Spark 3.5.0) et configurer SPARK_HOME.
2. Télécharger et sampler le dataset (`scripts/sample_data.sh`).
3. Préprocesser : `spark-submit scripts/preprocess.py --input data/edges_1pc.tsv --out parquet/`
4. Lancer PageRank RDD :
`spark-submit --master local[*] pagerank_rdd.py --input parquet/adj_1pc --nodes parquet/nodes_1pc --out parquet/pagerank_1pc --num-iters 10 --num-parts 200`
5. Lancer PageRank DataFrame analogue.

## Résultats et reporting
- Sauvegarder `experiments/results.csv` et `experiments/plots/*`.
- Inclure les screenshots Spark UI et les event logs.
