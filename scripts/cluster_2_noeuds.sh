#!/bin/bash
CLUSTER_NAME="pr-2n"
REGION="us-central1"
MACHINE_TYPE="n1-standard-2" 
DISK_SIZE="50GB"
WORKER_COUNT=1 # 1 Master + 1 Worker

echo "### 1A. Démarrage Cluster 2 Nœuds (${CLUSTER_NAME}) ###"
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region=${REGION} \
    --master-machine-type=${MACHINE_TYPE} \
    --worker-machine-type=${MACHINE_TYPE} \
    --num-workers=${WORKER_COUNT} \
    --num-secondary-workers=0 \
    --master-boot-disk-size=${DISK_SIZE} \
    --worker-boot-disk-size=${DISK_SIZE} \
    --image-version=2.1-debian11

echo "### 1B. Exécution des Jobs sur ${CLUSTER_NAME} ###"

# Exécution PageRank RDD
echo "--- Exécution PageRank RDD ---"
gcloud dataproc jobs submit pyspark ${GCS_BUCKET}/scripts/pagerank_rdd.py \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION}

# Exécution PageRank DataFrame
echo "--- Exécution PageRank DataFrame ---"
gcloud dataproc jobs submit pyspark ${GCS_BUCKET}/scripts/pagerank_df.py \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION}

echo "### 1C. Destruction du cluster ${CLUSTER_NAME} ###"
gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet
