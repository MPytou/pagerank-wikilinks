from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, when, coalesce
import time

spark = SparkSession.builder.appName("PageRankDF_Optimized").getOrCreate()

# -- 1. Préparation des Données --

# Charger edges (links)
# Assurez-vous que le chemin et le format sont corrects
edges = spark.read.csv("data/edges10.tsv", sep="\t", header=False, inferSchema=False).toDF("src", "dst")

# Calculer le nombre total de nœuds
nodes_list = edges.select(col("src")).union(edges.select(col("dst"))).distinct()
N = nodes_list.count()
DAMPING_FACTOR = 0.85
NUM_ITERS = 10

# Calculer les degrés sortants (Out-Degrees)
out_degrees = edges.groupBy("src").agg(count("dst").alias("out_degree"))

# -- 2. Initialisation --

# Initialisation : PageRank = 1/N pour tous les nœuds.
# Le DataFrame des ranks contient toutes les URIs de nœuds.
ranks = nodes_list.withColumn("rank", lit(1.0 / N)).withColumnRenamed("src", "uri")

# Joindre les liens aux degrés sortants (structure d'adjacence pré-calculée)
# df_links: (src, dst, out_degree)
df_links = edges.join(out_degrees, on="src")

start = time.time()

# -- 3. Boucle PageRank (Utilisation exclusive de DataFrames) --

for i in range(NUM_ITERS):
    # Jointure pour calculer les contributions
    # Calculer (Rank_Source / Out_Degree_Source) et attribuer à la Destination
    
    # 3.1 Joindre les ranks actuels (ranks) avec la table des liens (df_links)
    contribs = df_links.join(ranks, df_links.src == ranks.uri) \
                       .withColumn("contribution", col("rank") / col("out_degree")) \
                       .select(col("dst").alias("uri"), "contribution")

    # 3.2 Agréger les contributions par nœud cible (dst)
    # L'agrégation se fait entièrement via Spark SQL (groupBy)
    new_ranks_sum = contribs.groupBy("uri").agg(sum_("contribution").alias("rank_sum"))

    # 3.3 Appliquer la formule PageRank : PR_nouveau = (1-d) + d * Sum(PR_recu)
    ranks = new_ranks_sum.withColumn("rank", lit(1.0 - DAMPING_FACTOR) + lit(DAMPING_FACTOR) * col("rank_sum")) \
                         .select("uri", "rank")
    
    # Gérer les noeuds sans liens entrants (ils conservent le rank initial par défaut)
    # Nous joignons les ranks calculés (new_ranks_sum) aux nœuds initiaux (nodes_list) pour s'assurer qu'aucun nœud n'est perdu.
    ranks = nodes_list.select(col("src").alias("uri")).join(ranks, on="uri", how="left_outer") \
                      .withColumn("rank", coalesce(col("rank"), lit(1.0 / N))) \
                      .select("uri", "rank")

    print(f"Iteration {i+1} complète. Nombre de nœuds dans les ranks: {ranks.count()}")

end = time.time()
print(f"Temps total pour {NUM_ITERS} itérations:", end - start, "secondes")

# -- 4. Top PageRank --
print("\nTop 10 nœuds avec URI et PageRank :")
ranks.orderBy(col("rank").desc()).limit(10).show(truncate=False)

spark.stop()
