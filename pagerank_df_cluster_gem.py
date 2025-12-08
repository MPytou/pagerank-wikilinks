import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, size

# --- Configuration ---
# Chemin GCS corrigé
GCS_INPUT_PATH = "data/edges_1pc.tsv"
ITERATIONS = 10
DAMPING_FACTOR = 0.85

if __name__ == "__main__":
    # Initialisation de la session Spark
    spark = SparkSession.builder.appName("PageRankDataFrame").getOrCreate()
    
    start_time = time.time()

    # 1. Chargement et préparation des données (DataFrame)
    # Lit le fichier TSV en utilisant le chemin GCS
    edges = spark.read.csv(GCS_INPUT_PATH, sep="\t", header=False, inferSchema=False).toDF("src", "dst")
    
    # Création du DataFrame des liens : (source, [cible1, cible2, ...], out_degree)
    links = edges.groupBy("src").agg(
        col("src"), 
        _sum(lit(1)).alias("out_degree")
    ).cache()

    # Récupération de tous les nœuds uniques
    nodes = edges.select("src").union(edges.select("dst")).distinct().cache()
    num_nodes = nodes.count()

    # Jointure pour obtenir le degré de sortie pour chaque lien
    links_with_degree = edges.join(links.select("src", "out_degree"), on="src", how="left")

    # Initialisation des Ranks: (node, rank_initial)
    ranks = nodes.withColumn("rank", lit(1.0)).withColumnRenamed("src", "node").cache()

    print(f"Démarrage PageRank DataFrame avec {num_nodes} nœuds pour {ITERATIONS} itérations...")

    # 2. Boucle d'itération PageRank
    for i in range(ITERATIONS):
        
        # Jointure des liens et des ranks pour calculer la contribution
        # (src, dst, out_degree, current_rank)
        contributions = links_with_degree.join(
            ranks.withColumnRenamed("node", "src_join"), 
            links_with_degree["src"] == col("src_join")
        ).select(
            col("dst").alias("target"),
            (col("rank") / col("out_degree")).alias("contribution")
        ).groupBy("target").agg(_sum("contribution").alias("rank_sum"))

        # Calcul du nouveau Rank
        # Application du facteur d'amortissement
        new_ranks = contributions.withColumn(
            "new_rank",
            lit(DAMPING_FACTOR) * col("rank_sum") + lit(1.0 - DAMPING_FACTOR) / lit(num_nodes)
        ).select(col("target").alias("node"), col("new_rank").alias("rank"))

        # Mise à jour des Ranks (Jointure avec tous les nœuds pour les nœuds "dangling")
        ranks = ranks.select("node").join(
            new_ranks, on="node", how="left_outer"
        ).select(
            col("node"),
            col("rank").alias("old_rank"), # Le rank précédent
            col("new_rank").alias("rank") # Le nouveau rank (peut être null)
        ).withColumn(
            "rank", 
            # Si new_rank est null, garder l'ancien rank pour le calcul de l'amortissement
            col("rank").cast("float") 
        ).select("node", "rank").fillna(1.0 / num_nodes, subset=['rank']).cache()
        
        print(f"Iteration {i+1} complète.")
        
    end_time = time.time()

    # 3. Affichage des résultats
    top_10 = ranks.orderBy(col("rank").desc()).limit(10).collect()

    print("\n==========================================================")
    print("Résultats PageRank DataFrame")
    print("==========================================================")
    print("Top 10 nœuds avec PageRank :")
    for row in top_10:
        print(f"{row['node']}: {row['rank']}")
    
    print(f"\nTemps total d'exécution DataFrame : {end_time - start_time:.2f} secondes")
    print("==========================================================")
    
    spark.stop()
