#!/usr/bin/env python3
"""
PageRank using PySpark DataFrame
Usage: spark-submit pagerank_df.py --adj parquet/adj_1pc --nodes parquet/nodes_1pc --out parquet/pagerank_1pc_df --num-iters 10 --num-parts 200
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument('--adj', required=True)
parser.add_argument('--nodes', required=True)
parser.add_argument('--out', required=True)
parser.add_argument('--num-iters', type=int, default=10)
parser.add_argument('--num-parts', type=int, default=200)
parser.add_argument('--damping', type=float, default=0.85)
args = parser.parse_args()

spark = SparkSession.builder.appName('PageRank_DF').getOrCreate()

adj_df = spark.read.parquet(args.adj).repartition(args.num_parts, 'src_id').persist()

# init ranks DF
ranks = adj_df.select(F.col('src_id').alias('id')).withColumn('rank', F.lit(1.0)).repartition(args.num_parts, 'id').persist()

for i in range(args.num_iters):
# join and explode neighbors
joined = adj_df.join(ranks, adj_df.src_id == ranks.id)
exploded = joined.select('src_id', F.explode('neighbors').alias('dst'), (ranks.rank / F.size('neighbors')).alias('contrib'))
contribs = exploded.groupBy('dst').agg(F.sum('contrib').alias('sum_contrib'))
ranks = contribs.select(F.col('dst').alias('id'), (F.lit(1-args.damping) + args.damping * F.col('sum_contrib')).alias('rank')).repartition(args.num_parts, 'id').persist()
ranks.count()
print(f'Iteration {i+1} done')

ranks_df = ranks.toDF()
ranks_df.write.mode('overwrite').parquet(args.out)

nodes_df = spark.read.parquet(args.nodes)
joined = ranks_df.join(nodes_df, on='id', how='left')
joined.write.mode('overwrite').parquet(args.out + '_with_uri')
joined.orderBy('rank', ascending=False).limit(100).write.mode('overwrite').csv(args.out + '_top100_csv', header=True)

spark.stop()
