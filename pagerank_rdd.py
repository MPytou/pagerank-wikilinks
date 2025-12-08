import sys
import time
from pyspark.sql import SparkSession
from pyspark import StorageLevel

# CONFIGURATION
BUCKET = "gs://pagerank-data"
INPUT_DIR = f"{BUCKET}/data/edges_10pc.tsv"
ITERATIONS = 10
NUM_PARTITIONS = 200  

def compute_contribs(urls, rank):
    """Calcule la contribution envoyée à chaque voisin."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()
    sc = spark.sparkContext

    start_time = time.time()

    # 1. CHARGEMENT
    lines = spark.read.parquet(INPUT_DIR).rdd.map(lambda r: (r.src, r.dst))

    # 2. PREPARATION DU GRAPHE 
    links = lines.distinct(NUM_PARTITIONS) \
                 .groupByKey(NUM_PARTITIONS) \
                 .persist(StorageLevel.MEMORY_AND_DISK)

    # 3. INITIALISATION
    ranks = links.mapValues(lambda v: 1.0)

    # 4. BOUCLE PAGERANK
    for i in range(ITERATIONS):
        contribs = links.join(ranks).flatMap(
            lambda x: compute_contribs(x[1][0], x[1][1])
        )
        
        
        # On force reduceByKey à rester sur 200 partitions
        # Sinon gros risque de retomber sur le défaut (8 partitions) et crash
        ranks = contribs.reduceByKey(lambda x, y: x + y, numPartitions=NUM_PARTITIONS) \
                        .mapValues(lambda x: 0.15 + 0.85 * x)

    # 5. RESULTAT
    best = ranks.map(lambda x: (x[1], x[0])).max()
    
    end_time = time.time()
    
    print(f"TEMPS TOTAL (RDD): {end_time - start_time:.2f} secondes")
    print(f"GAGNANT: {best[1]} avec un score de {best[0]}")

    spark.stop()
