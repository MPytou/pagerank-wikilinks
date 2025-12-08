from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()

sc = spark.sparkContext

# Charger edges 10%
edges_rdd = sc.textFile("data/edges10.tsv") \
              .map(lambda line: line.split("\t")) \
              .map(lambda x: (x[0], x[1]))

start = time.time()

# Construire liste d’adjacence RDD
links = edges_rdd.groupByKey().mapValues(list).cache()

# Initialiser ranks
nodes = links.keys().distinct()
ranks = nodes.map(lambda x: (x, 1.0))

# PageRank iterations
for i in range(10):
    contribs = links.join(ranks).flatMap(lambda x: [(dst, x[1][1]/len(x[1][0])) for dst in x[1][0]])
    ranks = contribs.reduceByKey(lambda x,y: x+y).mapValues(lambda rank: 0.85*rank + 0.15)
    print(f"Iteration {i+1} complète")

end = time.time()

# Top 10
top_nodes = ranks.takeOrdered(10, key=lambda x: -x[1])
print("Top 10 nœuds avec PageRank :")
for node_id, rank in top_nodes:
    print(f"{node_id} : {rank}")

print("Temps total :", end - start, "secondes")

spark.stop()
