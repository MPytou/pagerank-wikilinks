import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count
from pyspark import StorageLevel

# CONFIGURATION
BUCKET = "gs://pagerank-data"

INPUT_DIR = f"{BUCKET}/data/edges_10pc"
ITERATIONS = 10

if __name__ == "__main__":
    # Initialize the Spark Session
    spark = SparkSession.builder.appName("PageRankDataFrame").getOrCreate()

    # Disable BroadcastJoin to prevent RAM explosion on smaller machines.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    # Tell Spark to prefer SortMergeJoin, which is often more stable for large-scale joins.
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

    start_time = time.time()

    # 1. DATA LOADING
    links = spark.read.parquet(INPUT_DIR)

    # 2. PRE-CALCULATION & PARTITIONING 
    # Calculate Out-Degrees
    out_degrees = links.groupBy("src").agg(count("*").alias("out_degree"))
    
    # Join
    links_with_degree = links.join(out_degrees, "src")
    
    # Repartition by "src" key. ANd shuffle before the loop.
    links_partitioned = links_with_degree.repartition(200, "src")
    
    # Persist the static link structure to MEMORY_AND_DISK.
    # This ensures the data is readily available for all 10 iterations.
    links_partitioned.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Force computation now so that the 'preparation' time is not counted in the loop
    # and to ensure the cache is filled before the loop starts.
    count_links = links_partitioned.count()
    print(f"Graph chargé avec {count_links} liens.")

    # 3. RANK INITIALIZATION
    # Get all unique nodes and initialize their rank to 1.0
    ranks = links_partitioned.select("src").distinct().withColumn("rank", lit(1.0))
    # Note: Ranks DataFrame is not partitioned yet, its partitioning will be optimized
    # by the SortMergeJoin logic during the loop.

    # 4. PAGERANK LOOP
    print("Début du calcul itératif...")
    loop_start = time.time()
    
    for i in range(ITERATIONS):
        # 4a. CALCULATE CONTRIBUTIONS
        # Join links (static, partitioned) with ranks (dynamic) on 'src'
        # The join should benefit from the initial partitioning of links_partitioned
        contributions = links_partitioned.join(ranks, "src") \
            .select(col("dst").alias("page"), (col("rank") / col("out_degree")).alias("contribution"))
        
        # 4b. AGGREGATE AND UPDATE RANKS
        # Group by the target page ('page') and sum the contributions. This requires a Shuffle.
        # Apply the PageRank formula: (1 - DAMPING_FACTOR) + DAMPING_FACTOR * sum(contributions)
        # Note: Damping factor calculation in this line is simplified: (RANDOM_JUMP + DAMPING_FACTOR * sum)
        ranks = contributions.groupBy("page").sum("contribution") \
            .withColumn("rank", lit(RANDOM_JUMP) + lit(DAMPING_FACTOR) * col("sum(contribution)")) \
            .select(col("page").alias("src"), "rank") # Rename 'page' back to 'src' for the next join
        
        print(f"Iteration {i+1} complete.")

    # 5. RESULTS
    # Find the top ranked page
    best = ranks.orderBy(col("rank").desc()).first()
    
    end_time = time.time()

    print(f" LOOP TIME (DataFrame): {end_time - loop_start:.2f} seconds")
    print(f" TOTAL TIME (Inc. Loading): {end_time - start_time:.2f} seconds")
    
    if best:
        print(f" WINNER: {best['src']} with a score of {best['rank']}")

    spark.stop()
