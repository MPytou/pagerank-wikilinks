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
   - `gsutil cp data/edges_10pc.tsv gs://pagerank-data/data/`
   - `gsutil cp pagerank_rdd.py gs://pagerank-data/scripts/`
   - `gsutil cp pagerank_df.py gs://pagerank-data/scripts/`
4. Exécutions des pagerank sur les clusters :
   - `./scripts/cluster_2_noeuds.sh`
   - `./scripts/cluster_4_noeuds.sh`
   - `./scripts/cluster_6_noeuds.sh`
5. Si un script bash ne veux pas s'éxécuter a cause des permissions :
   `chmod +x ./chemin/script.sh`
   
## Résultats
| Configuration | DataFrame | RDD     |
|---------------|-----------|---------|
| 2 nœuds       | 1759.75sec  | 1735.25sec| 
| 4 nœuds       | 538.68sec   | 936.21sec | 
| 6 nœuds       | 449.80sec   | 790.06sec | 

Top PageRank -> Category:Living_people with a score of 2319.162520 pour DF et RDD.
 
## Observations
- Performance DataFrame vs RDD => De meilleur performances pour le Dataframe que pour le RDD
- Influence du nombre de nœuds sur le temps => Comme attendu, plus il y a de noeuds et plus le temps est faible
- Top entité Wikipedia : `Category:Living_people` pour 10% du dataset, il aurait fallu faire les tests avec 100% du dataset pour avoir la réponse finale
  mais j'ai eu quelques problème avec le shuffle.

## Notes
   Lors de l'éxécution de ./scripts/sample_data.sh, il y a une étape de transformation d'un fichier ttv a tsv, pour qe spark puisse l'utiliser.
   Ce qui nécéssitent un petit traitement en amont.
   De plus, j'ai choisi de ne prendre que 10% du dataset vue la taille entière.

   Pour les machines du cluster :
   - REGION = "us-central1" => lieu ou j'ai stocké mon datatstore afin de minimiser les flux et temps de latence
   - MACHINE_TYPE = "n1-standard-2" => fournit 2 coeurs par noeud, avec une mémoire suffisante pour les tests et un coût faible pour éviter
   - DISK_SIZE = "50GB" => pas besoin de plus et permet de couter moins chère
