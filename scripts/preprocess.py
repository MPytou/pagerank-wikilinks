#!/usr/bin/env python3
"""
Preprocess: edges.tsv -> nodes.parquet, edges_id.parquet, adj.parquet
Usage: spark-submit scripts/preprocess.py --input data/edges_1pc.tsv --out parquet/ --num-parts 200
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, collect_list

parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--out', required=True)
parser.add_argument('--num-parts', type=int, default=200)
args = parser.parse_args()

spark = SparkSession.builder.appName('Preprocess').getOrCreate()

# read edges
edges = spark.read.csv(args.input, sep='\t').toDF('src','dst')

# nodes mapping
nodes = edges.select(col('src').alias('uri')).union(edges.select(col('dst').alias('uri'))).distinct()
nodes = nodes.withColumn('id', monotonically_increasing_id())

# join to get ids
edges_id = edges.join(nodes.withColumnRenamed('uri','src').withColumnRenamed('id','src_id'), on='src') \
.join(nodes.withColumnRenamed('uri','dst').withColumnRenamed('id','dst_id'), on='dst') \
.select('src_id','dst_id')

# adjacency
adj = edges_id.groupBy('src_id').agg(collect_list('dst_id').alias('neighbors'))

# write
nodes.repartition(args.num_parts).write.mode('overwrite').parquet(args.out.rstrip('/') + '/nodes')
edges_id.repartition(args.num_parts).write.mode('overwrite').parquet(args.out.rstrip('/') + '/edges_id')
adj.repartition(args.num_parts, 'src_id').write.mode('overwrite').parquet(args.out.rstrip('/') + '/adj')

spark.stop()
