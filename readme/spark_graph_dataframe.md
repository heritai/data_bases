# Social Network Graph Analysis with Spark DataFrames

This project demonstrates building and analyzing graphs from social network data using Spark DataFrames.

## Project Overview

The project analyzes a music listening dataset to build graphs representing relationships between users, tracks, and artists. The main steps include:

1.  **Data Loading and Preparation:** Loading the music dataset from CSV files into Spark DataFrames.
2.  **Graph Construction:** Building user-track, user-artist, artist-track, and track-track graphs.
3.  **Weight Calculation and Normalization:** Calculating normalized weights for edges in the graphs.
4.  **Triangle Calculation:** Implementing an improved triangle counting algorithm on the track-track graph.

## Data Sources

*   **data.csv:** Contains user listening history, linking users to tracks and artists.
    *   Columns: `userId`, `trackId`, `artistId`, `timestamp`
*   **meta.csv:** Contains metadata for tracks and artists (names, types, etc.).
    *   Columns: `type`, `Name`, `Artist`, `Id`

Both datasets are loaded from the `/musique` directory within the configured `DATASET_DIR`.

## Core Components

### 1. `calcul_weights` Function

```python
def calcul_weights(df, source, poids, n):
    """
    Calculates normalized weights for graph relationships.
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
```

This function calculates normalized weights for graph edges. It:

*   Partitions the data by the `source` node.
*   Ranks edges based on their initial `poids` (weight).
*   Keeps only the top `n` edges for each source.
*   Normalizes the weights so they sum to 1 for each source.

### 2. Graph Construction Functions

These functions create DataFrames representing different graphs:

*   **`construct_user_track_graph(data, n=100)`:** Creates a graph of users and the tracks they listen to.

    ```python
    userTrack = data.groupBy("userId", "trackId").count()
    userTrack = calcul_weights(userTrack, "userId", "count", n)
    ```

*   **`construct_user_artist_graph(data, n=100)`:** Creates a graph of users and the artists they listen to.

    ```python
    userArtist = data.groupBy("userId", "artistId").count()
    userArtist = calcul_weights(userArtist, "userId", "count", n)
    ```

*   **`construct_artist_track_graph(data, n=100)`:** Creates a graph of artists and their tracks.

    ```python
    artistTrack = data.groupBy("artistId", "trackId").count()
    artistTrack = calcul_weights(artistTrack, "artistId", "count", n)
    ```

*   **`construct_track_track_graph(data, time_window=10, n=100)`:** Creates a graph of tracks co-listened within a time window.

    ```python
    data2 = data.withColumnRenamed("trackId", "trackId2") \
                .withColumnRenamed("artistId", "artistId2") \
                .withColumnRenamed("timestamp", "timestamp2")

    trackTrack = data.join(data2, on="userId")
    trackTrack = trackTrack.filter(F.abs(col("timestamp")/60 - col("timestamp2")/60) < time_window)
    trackTrack = trackTrack.groupBy(col("trackId"), col("trackId2")).count()
    trackTrack = calcul_weights(trackTrack, "trackId", "count", n)
    ```

### 3. Triangle Calculation

This section implements an improved algorithm for calculating triangles in the track-track graph.

The parsing of strings
```python
def parse_string(users):
  results=[]
  i=0
  for i in range(len(users)-1):
    for j in range(i+1,len(users)):
      results.append([users[i],users[j]])
  print(results)
  return results

parse_string_udf = udf(parse_string, ArrayType(ArrayType(StringType())))
```

*   This code defines an function named `parse_string_udf` used in a latter code to manipulate the strings.

The following actions are performed on the table:

*   Creation of trackIds
*   Applying grouping using distinct data
*   Applying the parse_string_udf function to return the lists of its neighbor tuples
*   Mapping the exploded functions to the actual data
```python
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

```

## Next Steps

*   Complete the final graph construction.
*   Implement the personalized PageRank (PPR) algorithm.
*   Further explore relationships within the dataset.

This project provides a foundation for analyzing social network data with Spark DataFrames, demonstrating key techniques for graph construction, weight calculation, and community detection.
