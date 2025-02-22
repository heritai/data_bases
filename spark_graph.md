# Graph Analysis with Spark DataFrames

This document explains the code in `spark_graph.py`, which performs graph analysis on a music dataset using Spark DataFrames. It covers the problem description, code structure, and explanations of key parts of the code.

## Problem Description

The goal of this exercise (based on `TME1_graphes_DataFrame.pdf`) is to construct and analyze graphs representing relationships between users, tracks (songs), and artists in a music listening dataset. The key tasks include:

1.  **Data Loading and Preparation:** Loading the dataset from CSV files into Spark DataFrames.
2.  **Graph Construction:** Building various graphs, including user-track, user-artist, artist-track, and track-track graphs.  These graphs represent relationships between entities with weighted edges.  The weights are based on listening counts and co-occurrence.
3.  **Weight Normalization:** Normalizing the edge weights to represent the relative importance of each connection.
4.  **Triangle Calculation:** Calculating the number of triangles in the track-track graph, which indicates the interconnectedness of songs.
5.  **Personalized PageRank (PPR):** (Incomplete in the original code) Implementing PPR to recommend songs to users based on their listening history.

## Code Structure

The `spark_graph.py` script is organized as follows:

1.  **Environment Setup:**  Configures the execution environment (Colaboratory, local, or PPTI) and sets up necessary environment variables.
2.  **Spark Session Initialization:**  Initializes a Spark session using a helper function (`demarrer_spark`).
3.  **Data Loading:** Loads the music dataset from `data.csv` and `meta.csv` into Spark DataFrames.
4.  **Helper Function (`calcul_poids`):**  A general-purpose function to calculate normalized weights for graph edges. It takes a DataFrame, source column name, weight column name, and a maximum number of edges to keep per source node.  It calculates a normalized weight (`norm_<poids>`) by dividing the original weight by the sum of weights for that source.
5.  **Graph Construction Functions:**
    *   `construct_user_track_graph`:  Builds a graph of users and the tracks they listen to.  Edge weights represent the number of times a user has listened to a track.
    *   `construct_user_artist_graph`: Builds a graph of users and the artists they listen to. Edge weights represent the number of times a user has listened to tracks by an artist.
    *   `construct_artist_track_graph`: Builds a graph of artists and the tracks they perform.  Edge weights represent the number of times a track by an artist has been listened to.
    *   `construct_track_track_graph`:  Builds a graph of tracks and other tracks listened to by the same users within a specified time window. Edge weights represent the number of users who listened to both tracks.
6.  **Final Graph Construction:** (Incomplete) Intended to combine all the individual graphs into a single, unified graph.
7.  **Personalized PageRank (PPR):** (Incomplete)  Intended to implement PPR for song recommendations.
8.  **Triangle Calculation:**  Calculates the number of triangles in the track-track graph using an optimized algorithm.

## Key Code Sections and Explanations

### 1. `calcul_poids` Function

```python
def calcul_poids(df, source, poids, n):
    window = Window.partitionBy(source).orderBy(col(poids).desc())

    filterDF = df.withColumn("row_number", row_number().over(window)) \
        .filter(col("row_number") <= n) \
        .drop(col("row_number"))

    tmpDF = filterDF.groupBy(col(source)).agg(sum(col(poids)).alias("sum_" + poids))

    finalDF = filterDF.join(tmpDF, source, "inner") \
        .withColumn("norm_" + poids, col(poids) / col("sum_" + poids)) \
        .cache()

    return finalDF
```

This function is crucial for normalizing the weights in each graph.  Here's a breakdown:

*   **`Window.partitionBy(source).orderBy(col(poids).desc())`:** Creates a window partitioned by the `source` column (e.g., `userId`) and ordered by the `poids` column (e.g., `count`) in descending order.  This allows us to rank the edges for each source node.
*   **`row_number().over(window)`:** Assigns a rank to each edge within the window, based on the weight.
*   **`filter(col("row_number") <= n)`:** Keeps only the top `n` edges for each source node.  This limits the number of connections and reduces the complexity of the graph.
*   **`groupBy(col(source)).agg(sum(col(poids)).alias("sum_" + poids))`:** Calculates the sum of the weights for each source node.
*   **`withColumn("norm_" + poids, col(poids) / col("sum_" + poids))`:** Calculates the normalized weight by dividing the original weight by the sum of weights for the source node.  This ensures that the weights for each source node sum to 1.
*   **`cache()`:** Caches the resulting DataFrame for faster access in subsequent operations. This is particularly important because this `finalDF` might be used multiple times later on.

### 2. Graph Construction Functions (e.g., `construct_user_track_graph`)

These functions follow a similar pattern:

1.  **Grouping:** Group the data by the relevant columns (e.g., `userId` and `trackId`) to count the number of interactions.
2.  **Weight Calculation:** Call the `calcul_poids` function to normalize the weights.
3.  **Filtering and Sorting:** Filter the graph to keep only the top N weighted edges (using `rank()` and `filter()`) and sort the results for display (using `orderBy()`).
4.  **Display:** Display the resulting DataFrame using `show()`.  The original notebook used `collect()` and printed the results, but this is not recommended for large DataFrames as it pulls all data to the driver node.

### 3. `construct_track_track_graph` Function

```python
def construct_track_track_graph(data, time_window=10, n=100, top_n_weights=5, limit=20):
    data2 = data.withColumnRenamed("trackId", "trackId2") \
                .withColumnRenamed("artistId", "artistId2") \
                .withColumnRenamed("timestamp", "timestamp2")

    trackTrack = data.join(data2, on="userId")
    trackTrack = trackTrack.filter(abs(col("timestamp")/60 - col("timestamp2")/60) < time_window)
    trackTrack = trackTrack.groupBy(col("trackId"), col("trackId2")).count()
    trackTrack = calcul_poids(trackTrack, "trackId", "count", n)

    window = Window.orderBy(col("norm_count").desc())

    trackTrackList = trackTrack.withColumn("position", F.rank().over(window)) \
        .filter(col("position") <= top_n_weights) \
        .orderBy(col("norm_count").desc(), col("trackId").asc(), col("trackId2").asc()) \
        .limit(limit)

    print("Track-Track Graph (Top {} Weights):".format(top_n_weights))
    trackTrackList.show()

    return trackTrack
```

This function constructs the track-track graph, which represents tracks listened to by the same users within a specified time window.

*   **Self-Join:** The `data` DataFrame is joined with itself on `userId` to find tracks listened to by the same users.
*   **Time Window Filtering:**  The `filter` operation ensures that only tracks listened to within the specified `time_window` (in minutes) are considered related.
*   **Grouping and Weight Calculation:** The data is grouped by `trackId` and `trackId2` to count co-occurrences, and `calcul_poids` is used to normalize the weights.

### 4. Triangle Calculation

```python
def parse_string(users):
  results = []
  i = 0
  for i in range(len(users) - 1):
    for j in range(i + 1, len(users)):
      results.append((users[i], users[j]))
  return results

parse_string_udf = udf(parse_string, ArrayType(ArrayType(StringType())))

#Map1 - cours
#Prendre en considération uniquement les couples ordonnés (trackId, track1) (trackId < track1)
trackOrd = trackTrack_graph.filter(col("trackId") < col("trackId2"))

neighbors = trackOrd.groupBy("trackId").agg(sort_array(collect_list("trackId2")).alias("neighbors"))

couples = neighbors.withColumn("couples", parse_string_udf(col("neighbors")))

couples_exploded = couples.withColumn("couple", F.explode(col("couples")))

couples_exploded = couples_exploded.withColumn("trackId1", col("couple")[0]).withColumn("trackId2", col("couple")[1])

trackTrack_graph_with_id = trackTrack_graph.withColumn("edge_id", concat(F.least(col("trackId"), col("trackId2")), lit("_"), F.greatest(col("trackId"), col("trackId2"))))

couples_exploded = couples_exploded.withColumn("couple_id", concat(F.least(col("trackId1"), col("trackId2")), lit("_"), F.greatest(col("trackId1"), col("trackId2"))))

liste = couples_exploded.join(trackTrack_graph_with_id, couples_exploded.couple_id == trackTrack_graph_with_id.edge_id, "inner")

triangles = liste.groupBy("trackId").agg(count("*").alias("nb_triangles")).orderBy(desc("nb_triangles"))

triangles.show()
```

This section implements an optimized algorithm for calculating triangles in the `trackTrack` graph.

*   **`parse_string` UDF:**  A user-defined function that takes a list of users and returns a list of ordered pairs (combinations of two users).
*   **`trackOrd`:** Filters the `trackTrack` graph to consider only ordered pairs of tracks (`trackId` < `trackId2`). This avoids double-counting triangles.
*   **`neighbors`:**  Groups the `trackOrd` DataFrame by `trackId` and collects a sorted list of neighboring tracks.
*   **`couples`:** Applies the `parse_string_udf` to the list of neighbors to generate a list of ordered pairs of neighbors (couples).
*    **`couples_exploded`:** Explodes the list of couples into separate rows
*    **`edge_id` & `couple_id`**:  Creates unique identifiers for each edge and couples for joining later on.
*   **`liste`:** Filters the couples and uses the original graph to include only couples that exist in the original graph
*   **`triangles`:**  Groups by `trackId` and counts the number of triangles, ordering the result by the number of triangles in descending order.

## Improvements Made

*   **Added Comments:**  Extensive comments have been added to explain the purpose of each code section and the logic behind the operations.
*   **Modularization:**  The code has been organized into functions to improve readability and maintainability.
*   **Function Docstrings:**  Docstrings have been added to each function to explain its purpose, arguments, and return value.
*   **Explicit Column Selection:** The code now explicitly selects the columns needed in each query instead of using `*`, which can improve performance and prevent issues if the table schema changes.
*   **Descriptive Variable Names:** Variable names have been improved to be more descriptive and easier to understand.
*   **Removed Unnecessary `collect()` Calls:** The code now uses `show()` to display DataFrames, which avoids pulling all data to the driver node.  The `collect()` calls were replaced with `show()` and the `.limit()` function
*   **Added Parameterization:**  Added more parameters to the functions. For example, `construct_user_track_graph` now accepts a `limit` parameter so that users can customize how many rows they want to see.
*   **Clearer Time Window Calculation:** Added an explanation in the comments for the formula to calculate time window

## Next Steps

*   **Complete PPR Implementation:**  The PPR implementation is incomplete and needs to be finished.  This involves implementing the iterative update formula for the recommendation scores.
*   **Test and Validate:**  The code needs to be thoroughly tested and validated to ensure that it produces correct results.
*   **Optimize Performance:**  The code can be further optimized for performance by using more efficient data structures and algorithms.
*   **Complete Final Graph Construction:** The final graph construction should be implemented to combine all the individual graphs into a single graph.

This `spark_graph.md` provides a comprehensive overview of the `spark_graph.py` script, including its purpose, structure, and key code sections. It also highlights the improvements that have been made and suggests areas for further development.  I'll revise this as we iterate.
