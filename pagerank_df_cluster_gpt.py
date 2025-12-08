from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, collect_list
import time

# Spark session cluster-ready
spark = SparkSession.builder \
    .appName("PageRankDFCluster") \
    .config("spark.sql.shuffle.partitions", "200") \  # Ajuster selon taille du cluster
    .getOrCreate()

# Charger edges (10% ou 1%)
edges_path = "data/edges10.tsv"  # changer pour edges.tsv pour 1%
edges = spark.read.csv(edges_path, sep="\t", inferSchema=False).toDF("src", "dst")

start = time.time()

# Construire noeuds avec ID
nodes = edges.select(col("src").alias("uri")).union(edges.select(col("dst").alias("uri"))).distinct()
nodes = nodes.withColumn("id", monotonically_increasing_id()).cache()

# Construire edges avec ID
edges_id = edges.join(nodes.withColumnRenamed("uri","src").withColumnRenamed("id","src_id"), on="src") \
                .join(nodes.withColumnRenamed("uri","dst").withColumnRenamed("id","dst_id"), on="dst") \
                .select("src_id","dst_id") \
                .cache()

# Construire liste d’adjacence sans shuffle inutile
adj = edges_id.groupBy("src_id").agg(collect_list("dst_id").alias("neighbors")).cache()

# Initialiser ranks
ranks = nodes.rdd.map(lambda row: (row['id'], 1.0))

# PageRank iterations
num_iter = 10
for i in range(num_iter):
    contribs = adj.rdd.flatMap(
        lambda row: [(dst, ranks.lookup(row['src_id'])[0]/len(row['neighbors'])) for dst in row['neighbors']]
    )
    ranks = contribs.reduceByKey(lambda x,y:x+y).mapValues(lambda rank: 0.85*rank + 0.15)
    print(f"Iteration {i+1}/{num_iter} complète")

end = time.time()

# Top 10
top_nodes = ranks.takeOrdered(10, key=lambda x: -x[1])
nodes_dict = nodes.rdd.map(lambda row: (row['id'], row['uri'])).collectAsMap()

print("Top 10 nœuds avec URI et PageRank :")
for node_id, rank in top_nodes:
    print(f"{node_id} -> {nodes_dict.get(node_id,'UNKNOWN')} : {rank}")

print("Temps total :", end - start, "secondes")

spark.stop()
