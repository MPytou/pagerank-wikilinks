import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count
from pyspark import StorageLevel

# CONFIGURATION
# BUCKET = "gs://your-bucket-name"
BUCKET = "gs://pagerbucket10"

INPUT_DIR = f"{BUCKET}/cleaned_data"
ITERATIONS = 10

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PageRankDataFrame").getOrCreate()

    # On désactive le Broadcast pour éviter l'explosion RAM sur les petites machines
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    # on lui dit de préférer le SortMergeJoin
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

    start_time = time.time()

    # 1. CHARGEMENT
    links = spark.read.parquet(INPUT_DIR)

    # 2. PRE-CALCUL & PARTITIONNEMENT 
    # On calcule les degrés sortants
    out_degrees = links.groupBy("src").agg(count("*").alias("out_degree"))
    
    # On joint.
    links_with_degree = links.join(out_degrees, "src")
    
    # On repartitionne par "src", Shuffle avant la boucle
    links_partitioned = links_with_degree.repartition(200, "src")
    
    # On persiste sur DISQUE et MEMOIRE.
    # Si ça ne tient pas en RAM, ça ira sur le disque sans planter
    links_partitioned.persist(StorageLevel.MEMORY_AND_DISK)
    
    # On force le calcul maintenant pour que le temps de "préparation" ne soit pas compté dans la boucle
    # et pour vérifier que le cache est bien rempli
    count_links = links_partitioned.count()
    print(f"Graph chargé avec {count_links} liens.")

    # 3. INITIALISATION DES RANGS
    ranks = links_partitioned.select("src").distinct().withColumn("rank", lit(1.0))

    # 4. BOUCLE PAGERANK
    print("Début du calcul itératif...")
    loop_start = time.time()
    
    for i in range(ITERATIONS):
        # Seul 'ranks' va bouger à travers le réseau.
        contributions = links_partitioned.join(ranks, "src") \
             .select(col("dst").alias("page"), (col("rank") / col("out_degree")).alias("contribution"))
        
        ranks = contributions.groupBy("page").sum("contribution") \
            .withColumn("rank", 0.15 + 0.85 * col("sum(contribution)")) \
            .select(col("page").alias("src"), "rank")

    # 5. RESULTAT
    best = ranks.orderBy(col("rank").desc()).first()
    
    end_time = time.time()

    print(f" TEMPS BOUCLE (DataFrame): {end_time - loop_start:.2f} secondes")
    print(f" TEMPS TOTAL (Inc. Chargement): {end_time - start_time:.2f} secondes")
    
    if best:
        print(f" GAGNANT: {best['src']} avec un score de {best['rank']}")

    spark.stop()
