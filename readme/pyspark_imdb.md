# PySpark IMDB Analysis

This document describes the `pyspark_imdb.py` script, which performs analysis on the IMDB dataset using PySpark. It covers the purpose, setup, data loading, example queries, and further analyses, based on the instructions in a database course

## Purpose

The primary goal of this script is to explore the IMDB dataset and extract meaningful insights through PySpark queries. This involves:

*   **Dataset Understanding:** Exploring and understanding the IMDB dataset schema and content.
*   **Data Loading and Preparation:** Loading data from CSV files into Spark DataFrames and preparing it for analysis.
*   **Exploratory Data Analysis (EDA):** Performing exploratory data analysis using Spark SQL queries to identify patterns and trends.
*   **Multi-Dimensional Analysis:** Aggregating data across multiple dimensions to gain deeper insights.
*   **Advanced Analytics Techniques:** Utilizing the `CUBE` operator, window functions, and data denormalization techniques.
*   **Visualization:** Presenting findings through visualizations using Matplotlib.

This analysis aims to provide a practical demonstration of PySpark's capabilities for large-scale data analysis.

## Setup

We start by setting up the execution environment. The `EXECUTION` variable determines the execution context (Colaboratory, local, or PPTI), and the script configures the environment variables accordingly. Ensure you have Spark, PySpark, and Matplotlib installed.



### Environment Configuration

```python
EXECUTION = 'colaboratory'  # Default execution mode
SPARK_VERSION = "3.0.1"      # Spark version used
# ... (Environment variable setup) ...
```

The `EXECUTION` variable allows the script to be adapted to different environments. The `SPARK_VERSION` variable ensures consistency in the Spark version used.

### Spark Session Initialization

```python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("IMDB Analysis") \
    .getOrCreate()
```

A `SparkSession` is created to interact with Spark. The `appName` provides a descriptive name for the Spark application.

## Data Loading

The `read_tables` function is responsible for loading the IMDB data into Spark DataFrames. This function reads CSV files from a specified directory and infers the schema for each table.

### `read_tables` Function

```python
def read_tables(spark, dir):
    """
    Reads the IMDB tables from CSV files and creates Spark DataFrames.

    Args:
        spark: The SparkSession.
        dir: The directory containing the CSV files.

    Returns:
        A tuple of 11 Spark DataFrames representing the IMDB tables.
    """
    # ... (Schema definitions and DataFrame creation for each table) ...
    return title, kind_type, movie_info, info_type, cast_info, role_type, name,  person_info, movie_companies, company_name, company_type
```

The `read_tables` function reads the IMDB data from CSV files and creates Spark DataFrames. This function defines the schemas for all the IMDB tables.

Here's an example of how the `title` DataFrame is created:

```python
schema_title = """
      id INT,
      title STRING,
      imdb_index STRING,
      kind_id INT,
      production_year INT,
      imdb_id INT,
      phonetic_code STRING,
      episode_id STRING,
      season_nr INT,
      episode_nr INT,
      series_years STRING,
      md5sum STRING
    """

title = spark.read.csv(path=dir + "title.csv", schema=schema_title).persist()
title.createOrReplaceTempView("Title") # Create a temporary view for SQL queries
```

*   The `schema_title` variable defines the schema for the `title` table, including column names and data types.
*   `spark.read.csv` reads the CSV file into a DataFrame based on the defined schema.
*   `.persist()` caches the DataFrame in memory for faster access in subsequent operations.
*   `createOrReplaceTempView("Title")` creates a temporary view named "Title," allowing the DataFrame to be queried using Spark SQL.

## Example Queries

The script includes a variety of example queries that demonstrate different analysis techniques using Spark SQL.

### Basic Exploration

```python
print("\nFilms Description:")
title.describe("kind_id", "production_year").show()

print("\nNumber of Films:")
spark.sql("SELECT count(*) FROM Title").show()
```

These queries provide a basic overview of the `title` table, including descriptive statistics and the total number of films.

### Aggregation Queries

```python
print("\nNumber of Films per Type:")
films_per_type = spark.sql("""
    SELECT kind_id, count(*) as nb
    FROM Title
    GROUP BY kind_id
    ORDER BY nb DESC
""")
films_per_type.show()
```

This query groups the `title` table by `kind_id` and counts the number of films for each type, providing insights into the distribution of film types in the dataset.

### Join Queries

```python
print("\nJoin Title and Kind_Type:")
title_kind_join = spark.sql("""
    SELECT k.kind, t.title, t.production_year
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    ORDER BY t.production_year DESC
""")
title_kind_join.show(10, False)
```

This query joins the `title` and `kind_type` tables to retrieve the film type, title, and production year for each film. The `JOIN` clause specifies the join condition (`t.kind_id = k.id`), and the `ORDER BY` clause sorts the results by production year.

### Analysis with Matplotlib

```python
import matplotlib.pyplot as plt

films_per_year_collected = films_per_year.collect()

tabYear = [x.production_year for x in films_per_year_collected]
tabNb = [x.nb for x in films_per_year_collected]

plt.bar(tabYear, tabNb)
plt.xlabel('year')
plt.ylabel('nb films')
plt.title('films per year')
plt.show()
```

This code uses Matplotlib to create a bar chart showing the number of films produced per year.

*   `films_per_year.collect()` retrieves the data from the `films_per_year` DataFrame as a list of rows.
*   List comprehensions are used to extract the `production_year` and `nb` values into separate lists.
*   `plt.bar()` creates the bar chart.
*   `plt.xlabel()`, `plt.ylabel()`, and `plt.title()` set the labels and title for the chart.
*   `plt.show()` displays the chart.

### Advanced Analysis Techniques

The script demonstrates several advanced analysis techniques using Spark SQL.

#### Aggregation on Multiple Dimensions

```python
multi_dim_agg = spark.sql("""
    SELECT k.kind, t.production_year, count(*) as nb_films
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    WHERE t.production_year IS NOT NULL
    GROUP BY k.kind, t.production_year
    ORDER BY k.kind, t.production_year DESC
""")
multi_dim_agg.show(10)
```

This query aggregates the data by both `kind` and `production_year` to provide a more detailed view of the number of films for each combination of film type and year.

#### CUBE Operator

```python
cube_query = spark.sql("""
    SELECT k.kind, t.production_year, count(*) AS nb_films
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    WHERE t.production_year IS NOT NULL
    GROUP BY k.kind, t.production_year WITH CUBE
    ORDER BY k.kind, t.production_year DESC
""")
cube_query.show(20)
```

The `CUBE` operator generates aggregations for all possible combinations of the specified columns (`kind` and `production_year`). This allows for calculating totals for each dimension individually and for all dimensions combined.  This is a powerful feature for exploratory data analysis.

#### Window Functions

```python
from pyspark.sql.functions import rank, desc
from pyspark.sql import Window

window_spec = Window.partitionBy("production_year").orderBy(desc("nb_films"))

ranked_films = films_per_year.withColumn("rank", rank().over(window_spec))
ranked_films.show(10)
```

This code uses window functions to rank films by year.

*   `Window.partitionBy("production_year")` defines a window that partitions the data by production year.
*   `orderBy(desc("nb_films"))` orders the films within each window by the number of films in descending order.
*   `rank().over(window_spec)` calculates the rank of each film within its window based on the order.

#### Data Denormalization

```python
movie_actors = spark.sql("""
    SELECT
        t.title,
        collect_list(n.name) AS actors
    FROM
        Title t
    JOIN
        Cast_Info ci ON t.id = ci.movie_id
    JOIN
        Name n ON ci.person_id = n.id
    GROUP BY
        t.title
""")
movie_actors.show(10, truncate=False)
```

This query denormalizes the data by creating a structure where each movie is associated with a list of actors. The `collect_list()` function aggregates the actor names into a list for each movie.  This can be useful for certain types of analysis, but it can also be resource-intensive.

#### Reusing Queries with Temp Views

```python
films_per_type.createOrReplaceTempView("FilmsPerType")
top_film_types = spark.sql("""
    SELECT kind_id, nb
    FROM FilmsPerType
    WHERE nb > 100  -- Example filter
    ORDER BY nb DESC
""")
top_film_types.show(10)
```

This example demonstrates how to create a temporary view from a DataFrame and then reuse that view in another query. This can simplify complex queries and make the code more readable.

## Conclusion

The `pyspark_imdb.py` script provides a comprehensive example of how to use PySpark to analyze the IMDB dataset. It demonstrates a variety of techniques, including basic exploration, aggregation, joins, visualization, and advanced analytics techniques. The examples provided serve as a starting point for further exploration and analysis of the IMDB dataset.
