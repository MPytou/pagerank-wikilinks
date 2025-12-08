import sys
import time
from pyspark.sql import SparkSession
from pyspark import StorageLevel

# CONFIGURATION
BUCKET = "gs://pagerank-data"
INPUT_DIR = f"{BUCKET}/data/edges_10pc.tsv"
ITERATIONS = 10
NUM_PARTITIONS = 200  

#Calculates the contribution sent to each neighbor
def compute_contribs(urls, rank):
    num_urls = len(urls)
    if num_urls == 0:
        # Dangling node contribution is typically distributed evenly among ALL nodes,
        # but for this simplified implementation, we yield nothing if no links.
        return
    for url in urls:
        yield (url, rank / num_urls)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()
    sc = spark.sparkContext

    start_time = time.time()

    # 1. DATA LOADING
    lines = spark.read.parquet(INPUT_DIR).rdd.map(lambda r: (r.src, r.dst))

    # 2. GRAPH PREPARATION
    links = lines.distinct(NUM_PARTITIONS) \
                 .groupByKey(NUM_PARTITIONS) \
                 .persist(StorageLevel.MEMORY_AND_DISK)

    # 3. INITIALIZATION
    ranks = links.mapValues(lambda v: 1.0)

    # 4. PAGERANK LOOP
    for i in range(ITERATIONS):
        # 4a. CALCULATE CONTRIBUTIONS
        # links.join(ranks) benefits from shared partitioning key/size (if groupByKey/mapValues are used properly)
        # However, a perfect co-partitioning is only guaranteed if ranks is explicitly partitionBy'd with links' partitioner.
        # Here we rely on the implicit partitioning from mapValues.
        contribs = links.join(ranks).flatMap(
            lambda x: compute_contribs(x[1][0], x[1][1])
        )
        
        # 4b. AGGREGATE AND UPDATE RANKS
        # Aggregate contributions and apply the PageRank formula: (1-D) + D * sum(contribs)
        # Crucially, force reduceByKey to stay on 200 partitions to prevent
        # the default collapse (usually 8 partitions), which can cause memory issues and shuffling delays.
        ranks = contribs.reduceByKey(lambda x, y: x + y, numPartitions=NUM_PARTITIONS) \
                        .mapValues(lambda x: 0.15 + 0.85 * x)

    # 5. RESULTS
    best = ranks.map(lambda x: (x[1], x[0])).max()
    
    end_time = time.time()
    
    print(f"TOTAL TIME (RDD): {end_time - start_time:.2f} seconds")
    print(f"WINNER: {best[1]} with a score of {best[0]}")

    spark.stop()
