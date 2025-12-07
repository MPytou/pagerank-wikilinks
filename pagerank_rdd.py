from pyspark.sql import SparkSession
import time

# --- Configuration ---
NUM_PARTITIONS = 200  # Nombre de partitions optimisé pour un cluster de 6 nœuds (à ajuster)
DAMPING_FACTOR = 0.85
NUM_ITERS = 10

spark = SparkSession.builder.appName("PageRankRDD_Optimized").getOrCreate()
sc = spark.sparkContext

print(f"Démarrage PageRank RDD avec {NUM_PARTITIONS} partitions...")

# --- 1. Chargement et Préparation des Données ---

# Charger edges 10%
edges_rdd = sc.textFile("data/edges10.tsv") \
              .map(lambda line: line.split("\t")) \
              .map(lambda x: (x[0], x[1]))

# Construire la liste d’adjacence (Links RDD)
# links: (src, [dst1, dst2, ...])
# Partitionnement initial des liens (Structure statique)
links = edges_rdd.groupByKey() \
                 .mapValues(list) \
                 .partitionBy(NUM_PARTITIONS) \
                 .cache()  # Cache le RDD statique réutilisé 10 fois

# Extraction du partitionneur pour garantir le co-partitionnement
partitioner = links.partitioner

# --- 2. Initialisation des Ranks ---

# Initialiser ranks à 1.0 (ou 1/N si vous voulez être exact, mais 1.0 simplifie la logique)
# Le Ranks RDD doit être CO-PARTITIONNÉ avec le Links RDD
nodes = links.keys().distinct()
ranks = nodes.map(lambda x: (x, 1.0)) \
             .partitionBy(NUM_PARTITIONS, partitioner) # Co-partitionnement initial

# Début du chronométrage
start = time.time()

# --- 3. Boucle PageRank RDD (avec Co-Partitionnement) ---

for i in range(NUM_ITERS):
    # Jointure des liens et des ranks.
    # Grâce au .partitionBy précédent, cette jointure est locale, sans shuffle !
    contribs_rdd = links.join(ranks) \
                        .flatMap(lambda x: [
                            (dst, x[1][1] / len(x[1][0])) 
                            # x[1][1] est le rank (valeur de ranks), x[1][0] est la liste des voisins (valeur de links)
                            for dst in x[1][0]
                        ])
    
    # Agrégation des contributions par nœud (cette étape nécessite un shuffle)
    ranks = contribs_rdd.reduceByKey(lambda x,y: x+y, numPartitions=NUM_PARTITIONS) \
                        .mapValues(lambda rank_sum: DAMPING_FACTOR * rank_sum + (1.0 - DAMPING_FACTOR)) \
                        .partitionBy(NUM_PARTITIONS, partitioner) # Re-partitionnement garanti pour la prochaine jointure locale
    
    print(f"Iteration {i+1} complète")

# Arrêt du chronométrage
end = time.time()

# --- 4. Résultats ---

# Affichage des 10 meilleurs nœuds
top_nodes = ranks.takeOrdered(10, key=lambda x: -x[1])

print("\nTop 10 nœuds avec PageRank :")
for node_id, rank in top_nodes:
    print(f"{node_id} : {rank}")

print("\nTemps total :", end - start, "secondes")

spark.stop()
