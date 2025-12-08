# PageRank 2025-2026 — TP "Large Scale Data Management"

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
2. Télécharger et sampler le dataset (`./scripts/sample_data.sh`).
3. Poussé les données sur le datastore (remplacer pagerank-data par le nom de votre bucket) :
   - gsutil cp data/edges_1pc.tsv gs://pagerank-data/data/
   - gsutil cp data/edges_10pc.tsv gs://pagerank-data/data/
4. Exécutions des pagerank sur les clusters :
   - `./scripts/cluster_2_noeuds.sh`
   - `./scripts/cluster_4_noeuds.sh`
   - `./scripts/cluster_6_noeuds.sh`
5. Si un script bash ne veux pas s'éxécuter a cause des permissions :
   `chmod +x chemin/script.sh`
   
## Résultats et reporting
- Sauvegarder `experiments/results.csv` et `experiments/plots/*`.
- Inclure les screenshots Spark UI et les event logs.

## Expériences
| Config | Implémentation | Temps (s) | Top PageRank |
|--------|----------------|-----------|--------------|
| 2 nœuds | DataFrame      | X         | Category:Living_people ... |
| 2 nœuds | RDD            | Y         | Category:Living_people ... |
| 4 nœuds | DataFrame      | X         | ... |
| 4 nœuds | RDD            | Y         | ... |
| 6 nœuds | DataFrame      | X         | ... |
| 6 nœuds | RDD            | Y         | ... |

## Observations
- Performance DataFrame vs RDD
- Influence du nombre de nœuds sur le temps
- Top entité Wikipedia : `Category:Living_people`
