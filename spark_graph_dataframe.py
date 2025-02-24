# -*- coding: utf-8 -*-
"""
Social Network Graph Analysis with Spark DataFrames

This project demonstrates graph analysis techniques on a social network dataset
using Spark DataFrames. It focuses on building and analyzing graphs representing
relationships between users, tracks, and artists.

"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number, sum, col, rank, explode, collect_list, concat, lit, size
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, StringType

# Execution mode (colaboratory, local, ppti)
EXECUTION = 'colaboratory'  # Default to colaboratory

# Spark version
SPARK_VERSION = "2.4.4"

if EXECUTION == 'colaboratory':
    HOME = "/content"
    DATASET_DIR = "/content"
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "{}/spark-{}-bin-hadoop2.7".format(HOME, SPARK_VERSION)
elif EXECUTION == 'local':
    HOME = os.environ["HOME"]
    DATASET_DIR = "{}/dataset".format(HOME)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "{}/spark-{}-bin-hadoop2.7".format(HOME, SPARK_VERSION)
elif EXECUTION == 'ppti':
    HOME = os.environ["HOME"]
    DATASET_DIR = "{}/dataset".format(HOME)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "/usr/local/spark-{}-bin-hadoop2.7".format(SPARK_VERSION)
else:
    print("Invalid value for EXECUTION:", EXECUTION)
    raise ValueError("Invalid execution environment.")

print('JAVA_HOME:', os.environ["JAVA_HOME"])
print('SPARK_HOME:', os.environ["SPARK_HOME"])
print('DATASET_DIR:', DATASET_DIR)
print('HOME:', HOME)

# Initialize Spark session
spark = SparkSession.builder.appName("GraphAnalysis").getOrCreate()

# --- Data Loading ---

# Schema for the 'data.csv' file
data_schema = """
    userId LONG,
    trackId LONG,
    artistId LONG,
    timestamp LONG
"""

# Load the 'data.csv' file
data = spark.read.format("csv").option("header", "true").schema(data_schema) \
    .load(DATASET_DIR + "/musique/data.csv").persist()

# Schema for the 'meta.csv' file
meta_schema = """
    type STRING,
    Name STRING,
    Artist STRING,
    Id LONG
"""

# Load the 'meta.csv' file
meta = spark.read.format("csv").option("header", "true").schema(meta_schema) \
    .load(DATASET_DIR + "/musique/meta.csv").persist()


# --- Helper Function: calcul_weights ---

def calcul_weights(df, source, poids, n):
    """
    Calculates normalized weights for relationships in a graph.

    Args:
        df: DataFrame containing the graph edges and initial weights.
        source: Name of the column containing the source node IDs.
        poids: Name of the column containing the initial weights.
        n: Maximum number of edges to keep for each source node.

    Returns:
        DataFrame with normalized weights ('norm_' + poids).
    """

    window = Window.partitionBy(source).orderBy(F.col(poids).desc())

    filterDF = df.withColumn("row_number", row_number().over(window)) \
        .filter(col("row_number") <= n) \
        .drop(col("row_number"))

    tmpDF = filterDF.groupBy(col(source)).agg(sum(col(poids)).alias("sum_" + poids))

    finalDF = filterDF.join(tmpDF, source, "inner") \
        .withColumn("norm_" + poids, col(poids) / col("sum_" + poids)) \
        .cache()

    return finalDF

# --- Graph Construction ---

def display_graph(graph, top_n_weights=5, limit=20):
    """
    Helper function to display graph results.
    Filters for the top N weights and sorts for display.
    """
    window = Window.orderBy(col("norm_count").desc())
    graph_list = graph.withColumn("position", F.rank().over(window)) \
                      .filter(col("position") <= top_n_weights) \
                      .orderBy(col("norm_count").desc(), col("userId").asc(), col("trackId").asc()) \
                      .limit(limit)
    graph_list.show()
    return graph_list

def construct_user_track_graph(data, n=100):
    """
    Constructs a user-track graph from the data, calculating normalized weights.
    """
    userTrack = data.groupBy("userId", "trackId").count()
    userTrack = calcul_weights(userTrack, "userId", "count", n)
    return userTrack

def construct_user_artist_graph(data, n=100):
    """
    Constructs a user-artist graph from the data, calculating normalized weights.
    """
    userArtist = data.groupBy("userId", "artistId").count()
    userArtist = calcul_weights(userArtist, "userId", "count", n)
    return userArtist

def construct_artist_track_graph(data, n=100):
    """
    Constructs an artist-track graph from the data, calculating normalized weights.
    """
    artistTrack = data.groupBy("artistId", "trackId").count()
    artistTrack = calcul_weights(artistTrack, "artistId", "count", n)
    return artistTrack

def construct_track_track_graph(data, time_window=10, n=100):
    """
    Constructs a track-track graph based on co-occurrence within a time window.
    """
    data2 = data.withColumnRenamed("trackId", "trackId2") \
                .withColumnRenamed("artistId", "artistId2") \
                .withColumnRenamed("timestamp", "timestamp2")

    trackTrack = data.join(data2, on="userId")
    trackTrack = trackTrack.filter(F.abs(col("timestamp")/60 - col("timestamp2")/60) < time_window)
    trackTrack = trackTrack.groupBy(col("trackId"), col("trackId2")).count()
    trackTrack = calcul_weights(trackTrack, "trackId", "count", n)
    return trackTrack

# --- Main Execution ---

# Construct the user-track graph and display it
userTrack_graph = construct_user_track_graph(data)
print("User-Track Graph:")
display_graph(userTrack_graph)

# Construct the user-artist graph and display it
userArtist_graph = construct_user_artist_graph(data)
print("\nUser-Artist Graph:")
display_graph(userArtist_graph, top_n_weights=5)

# Construct the artist-track graph and display it
artistTrack_graph = construct_artist_track_graph(data)
print("\nArtist-Track Graph:")
display_graph(artistTrack_graph)

# Construct the track-track graph and display it
trackTrack_graph = construct_track_track_graph(data)
print("\nTrack-Track Graph:")
display_graph(trackTrack_graph)

# --- Final Graph Construction ---
# Incomplete - Combine all graphs into a single graph.
print("\nFinal Graph Construction: Not fully implemented.")

# --- Personalized PageRank (PPR) ---
# Incomplete - Recommendation based on personalized pagerank.
print("\nPersonalized PageRank: Not fully implemented.")

# --- Triangle Calculation ---
# Now we can implement our improved triangle count algorithm

from pyspark.sql.functions import udf
from pyspark.sql.types import *

def parse_string(users):
  results=[]
  i=0
  for i in range(len(users)-1):
    for j in range(i+1,len(users)):
      results.append([users[i],users[j]])
  print(results)
  return results

parse_string_udf = udf(parse_string, ArrayType(ArrayType(StringType())))

from pyspark.sql.functions import collect_list, sort_array

#Map1 - cours
#Prendre en considération uniquement les couples ordonnés (trackId, track1) (trackId < track1)
trackOrd =  trackTrack_graph.filter(col("trackId") < col("trackId2")).drop(col("norm_count"))


#Reduce 1 - cours: Construire pour chaque trackId la liste de couples ordonnés de ses voisins
# a) regrouper les lignes par trackId en construisant la liste de voisins triés par ordre
#    croissant (utiliser sort_array)
neighbors = trackOrd.groupBy("trackId").agg(collect_list("trackId2").alias("neighbors"))
neighbors = neighbors.withColumn("sorted_neigh", sort_array(neighbors["neighbors"])).select(col("trackId"), col("sorted_neigh"))

# b) utiliser la fonction définie précédemment pour retourner la liste de couples de voisins
couples=  neighbors.select(col("trackId"), parse_string_udf("sorted_neigh").alias("neigh_couples"))\
    .withColumn("neigh_couple", explode(col('neigh_couples')))\
    .drop(col("neigh_couples"))

# Map2 + Reduce 2 - cours
# prendre en considération uniquement les lignes telles que les couples de voisins
# construits précédemment existent également dans le graphe
from pyspark.sql.functions import concat, lit, count, desc

liste = trackOrd.withColumn("couple", concat(lit("["), col("trackId"), lit(", "), col("trackId2"), lit("]")))\
    .drop(col("trackId")).drop("trackId2")

# Calculer le nombre de triangles pour chaque utilisateur et
# trier le résultat par le nombre de triangles décroissant
triangles = couples.join(liste, col("neigh_couple")==col("couple")).drop(col("neigh_couple"))\
    .groupBy(col("trackId")).agg(count(col("couple")).alias("nb_triangles"))\
    .orderBy(desc("nb_triangles"))


triangles.show()

print('End of the script!')
spark.stop()