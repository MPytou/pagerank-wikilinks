#!/usr/bin/env python3
"""
PageRank using PySpark RDD
Usage: spark-submit pagerank_rdd.py --input parquet/adj_1pc --nodes parquet/nodes_1pc --out parquet/pagerank_1pc --num-iters 10 --num-parts 200
"""
import argparse
from pyspark.sql import SparkSession
from pyspark import StorageLevel

parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True, help='Parquet adjacency input (src_id, neighbors)')
parser.add_argument('--nodes', required=True, help='Parquet nodes (id, uri)')
parser.add_argument('--out', required=True, help='Output prefix for ranks parquet/csv')
parser.add_argument('--num-iters', type=int, default=10)
parser.add_argument('--num-parts', type=int, default=200)
parser.add_argument('--damping', type=float, default=0.85)
args = parser.parse_args()

spark = SparkSession.builder.appName('PageRank_RDD').getOrCreate()
sc = spark.sparkContext

# Read adjacency
adj_df = spark.read.parquet(args.input)
# Convert to RDD: (src_id, [dst_id, ...])
adj_rdd = adj_df.rdd.map(lambda r: (r['src_id'], r['neighbors']))

# Partition RDD with HashPartitioner to keep partitioning stable
adj_rdd = adj_rdd.partitionBy(args.num_parts)
adj_rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

# initialize ranks
ranks = adj_rdd.mapValues(lambda _: 1.0).partitionBy(args.num_parts)

for i in range(args.num_iters):
# join adjacency with ranks - same partitioner -> avoids wide shuffle
contribs = adj_rdd.join(ranks).flatMap(
lambda x: [(dst, x[1][1] / float(len(x[1][0]))) for dst in x[1][0]]
)

ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(
lambda v: (1 - args.damping) + args.damping * v
).partitionBy(args.num_parts).persist(StorageLevel.MEMORY_AND_DISK_SER)

# Optional: materialize to measure iteration time
ranks.count()
print(f'Iteration {i+1} done')

# Save ranks
ranks_df = ranks.toDF(['id', 'pagerank'])
ranks_df.write.mode('overwrite').parquet(args.out)

# join with nodes to get URIs
nodes_df = spark.read.parquet(args.nodes)
joined = ranks_df.join(nodes_df, on='id', how='left')
joined.write.mode('overwrite').parquet(args.out + '_with_uri')


# optional CSV top100
joined.orderBy('pagerank', ascending=False).limit(100).write.mode('overwrite').csv(args.out + '_top100_csv', header=True)


spark.stop()
