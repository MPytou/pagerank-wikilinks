#!/bin/bash

# --- CONFIGURATION GLOBALE ---
GCS_BUCKET="gs://pagerank-data"
DATA_INPUT_PATH="${GCS_BUCKET}/data/edges_10pc.tsv"

CLUSTER_NAME="pr-6n"
REGION="us-central1"
MACHINE_TYPE="n1-standard-2" 
DISK_SIZE="50GB"
PRIMARY_WORKERS=6  

echo "### Démarrage Cluster 6 Nœuds (${CLUSTER_NAME}) ###"
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region=${REGION} \
    --master-machine-type=${MACHINE_TYPE} \
    --worker-machine-type=${MACHINE_TYPE} \
    --num-workers=${PRIMARY_WORKERS} \
    --num-secondary-workers=0 \
    --master-boot-disk-size=${DISK_SIZE} \
    --worker-boot-disk-size=${DISK_SIZE} \
    --image-version=2.1-debian11

echo "### Exécution des Jobs sur ${CLUSTER_NAME} ###"

# Exécution PageRank RDD
echo "--- Exécution PageRank RDD ---"
gcloud dataproc jobs submit pyspark ${GCS_BUCKET}/scripts/pagerank_rdd.py \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION} \
    -- \
    --input "${DATA_INPUT_PATH}" \
    --num-parts 200

# Exécution PageRank DataFrame
echo "--- Exécution PageRank DataFrame ---"
gcloud dataproc jobs submit pyspark ${GCS_BUCKET}/scripts/pagerank_df.py \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION} \
    -- \
    --input "${DATA_INPUT_PATH}" \
    --num-parts 200

echo "### Destruction du cluster ${CLUSTER_NAME} ###"
gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet
