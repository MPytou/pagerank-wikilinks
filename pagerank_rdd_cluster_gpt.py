from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("PageRankRDDCluster") \
    .getOrCreate()

sc = spark.sparkContext

edges_path = "data/edges10.tsv"  # changer pour edges.tsv pour 1%
edges_rdd = sc.textFile(edges_path).map(lambda line: line.split("\t")).map(lambda x: (x[0], x[1]))

start = time.time()

# Construire liste d’adjacence
links = edges_rdd.groupByKey().mapValues(list).partitionBy(200).cache()  # partition pour cluster

# Initialiser ranks
nodes = links.keys().distinct()
ranks = nodes.map(lambda x: (x, 1.0))

# PageRank iterations
num_iter = 10
for i in range(num_iter):
    contribs = links.join(ranks).flatMap(
        lambda x: [(dst, x[1][1]/len(x[1][0])) for dst in x[1][0]]
    )
    ranks = contribs.reduceByKey(lambda x,y: x+y).mapValues(lambda rank: 0.85*rank + 0.15)
    print(f"Iteration {i+1}/{num_iter} complète")

end = time.time()

# Top 10
top_nodes = ranks.takeOrdered(10, key=lambda x: -x[1])
print("Top 10 nœuds avec PageRank :")
for node_id, rank in top_nodes:
    print(f"{node_id} : {rank}")

print("Temps total :", end - start, "secondes")

spark.stop()
