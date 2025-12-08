import sys
import time
from pyspark.sql import SparkSession

# --- Configuration ---
# Chemin GCS corrigé
GCS_INPUT_PATH = "data/edges_1pc.tsv"
ITERATIONS = 10
DAMPING_FACTOR = 0.85 

if __name__ == "__main__":
    # Initialisation de la session Spark
    spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()
    sc = spark.sparkContext
    
    start_time = time.time()
    
    # 1. Chargement et préparation des données (RDD)
    # Lit le fichier TSV en utilisant le chemin GCS
    edges_rdd = sc.textFile(GCS_INPUT_PATH).map(
        lambda line: line.split('\t')
    ).cache() # Mise en cache des liens
    
    # Création de la structure de liens (Source -> [Cible1, Cible2, ...])
    # links est un RDD: (source, liste_de_cibles)
    links = edges_rdd.map(lambda x: (x[0], [x[1]])) \
                 .reduceByKey(lambda a, b: a + b) \
                 .cache()

    # Initialisation des Ranks: (source, rank_initial)
    # On prend toutes les sources et toutes les cibles pour s'assurer d'avoir tous les nœuds
    # Tous les nœuds commencent avec un rank de 1.0
    ranks = edges_rdd.flatMap(lambda x: [x[0], x[1]]) \
        .distinct() \
        .map(lambda node: (node, 1.0))

    # Nombre total de nœuds (pour le facteur d'amortissement)
    num_nodes = ranks.count()
    
    print(f"Démarrage PageRank RDD avec {num_nodes} nœuds pour {ITERATIONS} itérations...")

    # 2. Boucle d'itération PageRank
    for i in range(ITERATIONS):
        # Contribution: (cible, contribution_du_noeud_source)
        # Jointure des liens et des ranks pour calculer la contribution que chaque nœud envoie à ses cibles
        contributions = links.join(ranks).flatMap(
            lambda x: [(target, x[1][1] / len(x[1][0])) for target in x[1][0]]
        )

        # Calcul des nouveaux Ranks
        # Accumulation des contributions par cible, application du facteur d'amortissement
        # On utilise .leftOuterJoin(ranks) pour s'assurer que même les nœuds sans lien entrant (dangling nodes) gardent leur rank.
        new_ranks = contributions.reduceByKey(lambda a, b: a + b).mapValues(
            lambda rank: DAMPING_FACTOR * rank + (1.0 - DAMPING_FACTOR) / num_nodes
        )

        # Mise à jour des Ranks (y compris les nœuds qui n'ont pas reçu de contributions dans cette itération)
        ranks = ranks.leftOuterJoin(new_ranks).map(
            lambda x: (x[0], x[1][1] if x[1][1] is not None else x[1][0])
        ).cache() # Mise en cache pour la prochaine itération
        
        print(f"Iteration {i+1} complète.")
        
    end_time = time.time()
    
    # 3. Affichage des résultats
    top_10 = ranks.sortBy(lambda x: x[1], ascending=False).take(10)

    print("\n==========================================================")
    print("Résultats PageRank RDD")
    print("==========================================================")
    print("Top 10 nœuds avec PageRank :")
    for (node, rank) in top_10:
        print(f"{node}: {rank}")
    
    print(f"\nTemps total d'exécution RDD : {end_time - start_time:.2f} secondes")
    print("==========================================================")

    spark.stop()
