# -*- coding: utf-8 -*-
"""
Graphs in DataFrame

This script constructs and analyzes graphs using Spark DataFrames.  It focuses on
relationships within a music dataset, building graphs of users, tracks, and artists.

"""

import os

# Execution mode: colaboratory, local, or ppti
EXECUTION = 'colaboratory'  # Default to colaboratory for now.  Change as needed.

# Configuration based on execution environment
SPARK_VERSION = "2.4.4"  # Keep this as a constant.

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
    raise ValueError("Invalid execution environment.  Must be 'colaboratory', 'local', or 'ppti'.")


print('JAVA_HOME:', os.environ["JAVA_HOME"])
print('SPARK_HOME:', os.environ["SPARK_HOME"])
print('DATASET_DIR:', DATASET_DIR)
print('HOME:', HOME)

# Spark setup and download (removed for brevity, assuming already done)

# --- Spark Session ---
from demarrer_spark import demarrer_spark # Assuming this script exists and correctly starts Spark

spark = demarrer_spark()

# --- Data Loading ---
# Schema definitions
data_schema = """
    userId LONG,
    trackId LONG,
    artistId LONG,
    timestamp LONG
"""

meta_schema = """
    type STRING,
    Name STRING,
    Artist STRING,
    Id LONG
"""

# Load data
data = spark.read.format("csv").option("header", "true").schema(data_schema) \
    .load(DATASET_DIR + "/musique/data.csv").persist()

meta = spark.read.format("csv").option("header", "true").schema(meta_schema) \
    .load(DATASET_DIR + "/musique/meta.csv").persist()


# --- Helper Function: calcul_poids ---
from pyspark.sql.functions import row_number, sum, col
from pyspark.sql import functions as F
from pyspark.sql import Window


def calcul_poids(df, source, poids, n):
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

    window = Window.partitionBy(source).orderBy(col(poids).desc())

    filterDF = df.withColumn("row_number", row_number().over(window)) \
        .filter(col("row_number") <= n) \
        .drop(col("row_number"))

    tmpDF = filterDF.groupBy(col(source)).agg(sum(col(poids)).alias("sum_" + poids))

    finalDF = filterDF.join(tmpDF, source, "inner") \
        .withColumn("norm_" + poids, col(poids) / col("sum_" + poids)) \
        .cache()  # Cache the result for performance

    return finalDF


# --- Graph Construction ---

# 1. User-Track Graph
def construct_user_track_graph(data, n=100, top_n_weights=5, limit=20):
    """
    Constructs a user-track graph from the data, calculating normalized weights.

    Args:
        data: The input DataFrame (from data.csv).
        n: The number of tracks to keep per user based on weight.
        top_n_weights:  Keep only arcs with the top N weights
        limit: Number of rows to show in the final result.

    Returns:
        A DataFrame representing the user-track graph with normalized weights.
    """

    # Calculate the number of times each user listens to a track
    userTrack = data.groupBy("userId", "trackId").count()

    # Calculate normalized weights using the calcul_poids function
    userTrack = calcul_poids(userTrack, "userId", "count", n)

    # Filter for the top N weights and sort for display.
    window = Window.orderBy(col("norm_count").desc())

    userTrackList = userTrack.withColumn("position", F.rank().over(window)) \
        .filter(col("position") <= top_n_weights) \
        .orderBy(col("norm_count").desc(), col("userId").asc(), col("trackId").asc()) \
        .limit(limit) # Limit to top 20 rows for display.

    print("User-Track Graph (Top {} Weights):".format(top_n_weights))
    userTrackList.show()  # Use show() for DataFrame display

    # Alternative: collect and print as requested in the original notebook, but not recommended for large DataFrames
    # for val in userTrackList.collect():
    #    print("%s %s %s" % val)

    return userTrack

userTrack_graph = construct_user_track_graph(data)

# 2. User-Artist Graph
def construct_user_artist_graph(data, n=100, top_n_weights=5, limit=20):
    """
    Constructs a user-artist graph from the data, calculating normalized weights.

    Args:
        data: The input DataFrame (from data.csv).
        n: The number of artists to keep per user.
        top_n_weights:  Keep only arcs with the top N weights
        limit: Number of rows to show in the final result.

    Returns:
        A DataFrame representing the user-artist graph with normalized weights.
    """

    # Group data by userId and artistId to count listens
    userArtist = data.groupBy("userId", "artistId").count()

    # Calculate normalized weights
    userArtist = calcul_poids(userArtist, "userId", "count", n)

    # Filter for the top N weights and sort for display.
    window = Window.orderBy(col("norm_count").desc())

    userArtistList = userArtist.withColumn("position", F.rank().over(window)) \
        .filter(col("position") <= top_n_weights) \
        .orderBy(col("norm_count").desc(), col("userId").asc(), col("artistId").asc()) \
        .limit(limit) # Limit to top 20 rows for display.

    print("User-Artist Graph (Top {} Weights):".format(top_n_weights))
    userArtistList.show()

    # Alternative: collect and print as requested in the original notebook, but not recommended for large DataFrames
    # for val in userArtistList.collect():
    #    print("%s %s %s" % val)

    return userArtist

userArtist_graph = construct_user_artist_graph(data)

# 3. Artist-Track Graph
def construct_artist_track_graph(data, n=100, top_n_weights=5, limit=20):
    """
    Constructs an artist-track graph from the data, calculating normalized weights.

    Args:
        data: The input DataFrame (from data.csv).
        n: The number of tracks to keep per artist.
        top_n_weights: Keep only arcs with the top N weights
        limit: Number of rows to show in the final result.

    Returns:
        A DataFrame representing the artist-track graph with normalized weights.
    """

    # Group data by artistId and trackId
    artistTrack = data.groupBy("artistId", "trackId").count()

    # Calculate normalized weights
    artistTrack = calcul_poids(artistTrack, "artistId", "count", n)

    # Filter for the top N weights and sort for display.
    window = Window.orderBy(col("norm_count").desc())

    artistTrackList = artistTrack.withColumn("position", F.rank().over(window)) \
        .filter(col("position") <= top_n_weights) \
        .orderBy(col("norm_count").desc(), col("artistId").asc(), col("trackId").asc()) \
        .limit(limit) # Limit to top 20 rows for display

    print("Artist-Track Graph (Top {} Weights):".format(top_n_weights))
    artistTrackList.show()

    # Alternative: collect and print as requested in the original notebook, but not recommended for large DataFrames
    # for val in artistTrackList.collect():
    #    print("%s %s %s" % val)

    return artistTrack

artistTrack_graph = construct_artist_track_graph(data)

# 4. Track-Track Graph
from datetime import datetime
from pyspark.sql.functions import abs
def construct_track_track_graph(data, time_window=10, n=100, top_n_weights=5, limit=20):
    """
    Constructs a track-track graph from the data, calculating normalized weights
    based on co-occurrence within a time window.

    Args:
        data: The input DataFrame (from data.csv).
        time_window: The time window (in minutes) within which tracks must be
                     listened to by the same user to be considered related.
        n: The number of tracks to keep per track.
        top_n_weights:  Keep only arcs with the top N weights
        limit: Number of rows to show in the final result.

    Returns:
        A DataFrame representing the track-track graph with normalized weights.
    """

    # Create a self-join of the data DataFrame
    data2 = data.withColumnRenamed("trackId", "trackId2") \
                .withColumnRenamed("artistId", "artistId2") \
                .withColumnRenamed("timestamp", "timestamp2")

    # Join data with itself on userId
    trackTrack = data.join(data2, on="userId")

    # Filter for co-occurrences within the specified time window
    trackTrack = trackTrack.filter(abs(col("timestamp")/60 - col("timestamp2")/60) < time_window)

    # Group by trackId and trackId2 to count co-occurrences
    trackTrack = trackTrack.groupBy(col("trackId"), col("trackId2")).count()

    # Calculate normalized weights
    trackTrack = calcul_poids(trackTrack, "trackId", "count", n)

    # Filter for the top N weights and sort for display.
    window = Window.orderBy(col("norm_count").desc())

    trackTrackList = trackTrack.withColumn("position", F.rank().over(window)) \
        .filter(col("position") <= top_n_weights) \
        .orderBy(col("norm_count").desc(), col("trackId").asc(), col("trackId2").asc()) \
        .limit(limit) # Limit to top 20 rows for display.

    print("Track-Track Graph (Top {} Weights):".format(top_n_weights))
    trackTrackList.show()

    # Alternative: collect and print as requested in the original notebook, but not recommended for large DataFrames
    # for val in trackTrackList.collect():
    #    print("%s %s %s" % val)

    return trackTrack

trackTrack_graph = construct_track_track_graph(data)


# --- Final Graph Construction ---
# Combine all graphs into a single graph.

# Not implemented completely, because the code is incomplete

# --- Personalized PageRank (PPR) ---
# Recommendation based on personalized pagerank.

# Not implemented completely, because the code is incomplete

# --- Triangle Calculation ---
# Calculate triangles in the trackTrack graph.

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType

def parse_string(users):
  """
  Parses a list of users and returns a list of ordered pairs.

  Args:
    users: A list of users sorted in ascending order.

  Returns:
    A list of ordered pairs of users.
  """
  results = []
  i = 0
  for i in range(len(users) - 1):
    for j in range(i + 1, len(users)):
      results.append((users[i], users[j]))
  return results

parse_string_udf = udf(parse_string, ArrayType(ArrayType(StringType())))

# --- Implementation of the triangle calculation algorithm

from pyspark.sql.functions import collect_list, sort_array, array, size

#Map1 - cours
#Prendre en considération uniquement les couples ordonnés (trackId, track1) (trackId < track1)
trackOrd = trackTrack_graph.filter(col("trackId") < col("trackId2"))

#Reduce 1 - cours: Construire pour chaque trackId la liste de couples ordonnés de ses voisins
# a) regrouper les lignes par trackId en construisant la liste de voisins triés par ordre
#    croissant (utiliser sort_array)
neighbors = trackOrd.groupBy("trackId").agg(sort_array(collect_list("trackId2")).alias("neighbors"))

# b) utiliser la fonction définie précédemment pour retourner la liste de couples de voisins
couples = neighbors.withColumn("couples", parse_string_udf(col("neighbors")))

# Explode the couples array to have one row per couple
couples_exploded = couples.withColumn("couple", F.explode(col("couples")))

# Extract trackId1 and trackId2 from the couple
couples_exploded = couples_exploded.withColumn("trackId1", col("couple")[0]).withColumn("trackId2", col("couple")[1])

# Map2 + Reduce 2 - cours
# prendre en considération uniquement les lignes telles que les couples de voisins
# construits précédemment existent également dans le graphe
# Create a unique identifier for each edge in the original graph
trackTrack_graph_with_id = trackTrack_graph.withColumn("edge_id", concat(F.least(col("trackId"), col("trackId2")), lit("_"), F.greatest(col("trackId"), col("trackId2"))))

# Create a unique identifier for each couple
couples_exploded = couples_exploded.withColumn("couple_id", concat(F.least(col("trackId1"), col("trackId2")), lit("_"), F.greatest(col("trackId1"), col("trackId2"))))

# Join with the original graph to filter only the couples that exist in the graph
liste = couples_exploded.join(trackTrack_graph_with_id, couples_exploded.couple_id == trackTrack_graph_with_id.edge_id, "inner")

# Calculer le nombre de triangles pour chaque utilisateur et
# trier le résultat par le nombre de triangles décroissant
triangles = liste.groupBy("trackId").agg(count("*").alias("nb_triangles")).orderBy(desc("nb_triangles"))

triangles.show()
