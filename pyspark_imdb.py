# -*- coding: utf-8 -*-
"""
PySpark IMDB Analysis

This script performs analysis on the IMDB dataset using PySpark.  It loads data,
explores the dataset, and performs various queries to gain insights into the data.

"""

import os
from pyspark.sql import SparkSession  # Import SparkSession
from pyspark.sql.functions import rank, desc, collect_list  # Import functions explicitly
from pyspark.sql import Window
import matplotlib.pyplot as plt # Import matplotlib

# Execution mode: colaboratory, local, or ppti
EXECUTION = 'colaboratory'  # Default to colaboratory. Change as needed.

SPARK_VERSION = "3.0.1"  # Ensure consistent Spark version

# URL for public datasets
PUBLIC_DATASET_URL = "https://nuage.lip6.fr/s/H3bpyRGgnCq2NR4"
PUBLIC_DATASET = PUBLIC_DATASET_URL + "/download?path="

print("SPARK_VERSION", SPARK_VERSION)
print("URL for datasets ", PUBLIC_DATASET_URL)

# --- Java Version Check ---
!java -version  # Check Java version (ideally 11 or higher)

# --- Environment Setup ---
if EXECUTION == 'colaboratory':
    HOME = "/content"
    DATASET_DIR = "/content"
    os.environ["JAVA_HOME"] = "/usr"  # Corrected JAVA_HOME for Colab
    os.environ["SPARK_HOME"] = "{}/spark-{}-bin-hadoop2.7".format(HOME, SPARK_VERSION)

elif EXECUTION == 'local':
    HOME = os.environ["HOME"]
    DATASET_DIR = "{}/dataset".format(HOME)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "{}/spark-{}-bin-hadoop2.7".format(HOME, SPARK_VERSION)

elif EXECUTION == 'ppti':
    HOME = os.environ["HOME"]
    DATASET_DIR = "/Infos/bd/spark/dataset"
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "/usr/local/spark-{}-bin-hadoop2.7".format(SPARK_VERSION)
else:
    print("Invalid value for EXECUTION:", EXECUTION)
    raise ValueError("Invalid execution environment.  Must be 'colaboratory', 'local', or 'ppti'.")


print('JAVA_HOME:', os.environ["JAVA_HOME"])
print('SPARK_HOME:', os.environ["SPARK_HOME"])
print('DATASET_DIR:', DATASET_DIR)
print('HOME:', HOME)



# Create a SparkSession (if demarrer_spark is not available)
spark = SparkSession.builder \
    .appName("IMDB Analysis") \
    .getOrCreate()

# --- Data Loading ---

def read_tables(spark, dir):
    """
    Reads the IMDB tables from CSV files and creates Spark DataFrames.

    Args:
        spark: The SparkSession.
        dir: The directory containing the CSV files.

    Returns:
        A tuple of 11 Spark DataFrames representing the IMDB tables.
    """

    #==============
    # Title
    #==============
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

    title = spark.read.csv(path = dir + "title.csv", schema = schema_title).persist()
    title.createOrReplaceTempView("Title") # Create a temporary view for SQL queries
    #title.printSchema()


    #==============
    # Kind_Type
    #==============
    schema_kind_type = "id INT, kind STRING"

    kind_type = spark.read.csv(path = dir + "kind_type.csv", schema = schema_kind_type).persist()
    kind_type.createOrReplaceTempView("Kind_Type") # Create a temporary view for SQL queries
    #kind_type.printSchema()


    #==============
    # Movie_Info
    #==============
    schema_movie_info = """
        id int,
        movie_id int,
        info_type_id int,
        info string,
        note string
    """

    movie_info = spark.read.csv(path = dir + "movie_info.csv", schema = schema_movie_info).persist()
    movie_info.createOrReplaceTempView("Movie_Info") # Create a temporary view for SQL queries
    #movie_info.printSchema()


    #==============
    # Info_Type
    #==============
    schema_info_type = "id INT, info STRING"

    info_type = spark.read.csv(path = dir + "info_type.csv", schema = schema_info_type).persist()
    info_type.createOrReplaceTempView("Info_Type") # Create a temporary view for SQL queries
    #info_type.printSchema()


    #==============
    # Cast_Info
    #==============
    schema_cast_info = """
        id int,
        person_id int,
        movie_id int,
        person_role_id int,
        note string,
        nr_order int,
        role_id int
    """

    cast_info = spark.read.csv(path = dir + "cast_info.csv", schema = schema_cast_info).persist()
    cast_info.createOrReplaceTempView("Cast_Info") # Create a temporary view for SQL queries
    #cast_info.printSchema()


    #==============
    # Role_Type
    #==============
    schema_role_type = "id INT, role STRING"

    role_type = spark.read.csv(path = dir + "role_type.csv", schema = schema_role_type).persist()
    role_type.createOrReplaceTempView("Role_Type") # Create a temporary view for SQL queries
    #role_type.printSchema()



    #==============
    # Name
    #==============
    schema_name = """
        id int,
        name string,
        imdb_index string,
        imdb_id int,
        gender string,
        name_pcode_cf string,
        name_pcode_nf string,
        surname_pcode string,
        md5sum string
    """

    name = spark.read.csv(path = dir + "name.csv", schema = schema_name).persist()
    name.createOrReplaceTempView("Name") # Create a temporary view for SQL queries
    #name.printSchema()



    #==============
    # Person_Info
    #==============
    schema_person_info = """
        id int,
        person_id int,
        info_type_id int,
        info string,
        note string
    """

    person_info = spark.read.csv(path = dir + "person_info.csv", schema = schema_person_info).persist()
    person_info.createOrReplaceTempView("Person_Info") # Create a temporary view for SQL queries


    #==============
    # Movie_Companies
    #==============
    schema_movie_companies = """
        id int,
        movie_id int,
        company_id int,
        company_type_id int,
        note string
    """

    movie_companies = spark.read.csv(path = dir + "movie_companies.csv", schema = schema_movie_companies).persist()
    movie_companies.createOrReplaceTempView("Movie_Companies") # Create a temporary view for SQL queries
    #movie_companies.printSchema()


    #==============
    # Company_Name
    #==============
    schema_company_name = """
        id int,
        name string,
        country_code string,
        imdb_id int,
        name_pcode_nf string,
        name_pcode_sf string,
        md5sum string
    """

    company_name = spark.read.csv(path = dir + "company_name.csv", schema = schema_company_name)
    company_name.createOrReplaceTempView("Company_Name") # Create a temporary view for SQL queries
    #company_name.printSchema()

    #==============
    # Company_Type
    #==============
    schema_company_type = "id INT, kind STRING"

    company_type = spark.read.csv(path = dir + "company_type.csv", schema = schema_company_type).persist()
    company_type.createOrReplaceTempView("Company_Type") # Create a temporary view for SQL queries
    #company_type.printSchema()



    return title, kind_type, movie_info, info_type, cast_info, role_type, name,  person_info, movie_companies, company_name, company_type

# --- END read_tables function ---

# Load data
data_dir = "{}/imdb/vldb2015/csvfiles_sample001/".format(DATASET_DIR)
title, kind_type, movie_info, info_type, cast_info, role_type, name, \
person_info, movie_companies, company_name, company_type = read_tables(spark, data_dir)

print("Tables defined")

title.count()  # Example: Count the number of titles

# --- Example Queries ---

# 1. Films Description
print("\nFilms Description:")
title.describe("kind_id", "production_year").show()

# 2. Number of Films
print("\nNumber of Films:")
spark.sql("SELECT count(*) FROM Title").show()

# 3. Film Types
print("\nFilm Types:")
film_types = spark.sql("""
    SELECT distinct kind_id
    FROM Title
    ORDER BY kind_id
""")
film_types.show()

# 4. Number of Films per Type
print("\nNumber of Films per Type:")
films_per_type = spark.sql("""
    SELECT kind_id, count(*) as nb
    FROM Title
    GROUP BY kind_id
    ORDER BY nb DESC
""")
films_per_type.show()

# 5. Number of Films per Year
print("\nNumber of Films per Year:")
films_per_year = spark.sql("""
    SELECT production_year, count(*) as nb
    FROM Title
    WHERE production_year IS NOT NULL
    GROUP BY production_year
    ORDER BY nb DESC
""")
films_per_year.show(5)

# 6. Distribution of Number of Films (Skewed)
print("\nDistribution of Number of Films (Skewed):")
films_per_year_desc = films_per_year.describe('nb')
films_per_year_desc.show()

# 7. Visualize Number of Films per Year
films_per_year_collected = films_per_year.collect()

tabYear = [x.production_year for x in films_per_year_collected]
tabNb = [x.nb for x in films_per_year_collected]

plt.bar(tabYear, tabNb)
plt.xlabel('year')
plt.ylabel('nb films')
plt.title('films per year')
plt.show()

# 8. Kind Types (from kind_type table)
print("\nKind Types:")
kind_type.show()

# 9. Join Title and Kind_Type
print("\nJoin Title and Kind_Type:")
title_kind_join = spark.sql("""
    SELECT k.kind, t.title, t.production_year
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    ORDER BY t.production_year DESC
""")
title_kind_join.show(10, False)

# 10. Number of Films per Kind
print("\nNumber of Films per Kind:")
films_per_kind = spark.sql("""
    SELECT k.kind, count(*) as nb
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    GROUP BY k.kind
    ORDER BY count(*) DESC
""")
films_per_kind.show()

# --- Cast Info Analysis ---

# 11. Cast Info Schema and Count
print("\nCast Info Schema:")
cast_info.printSchema()
print("\nCast Info Count:", cast_info.count())

# 12. Number of Roles per Actor
print("\nNumber of Roles per Actor:")
roles_per_actor = spark.sql("""
    SELECT person_id, count(*) as nb
    FROM Cast_Info
    GROUP BY person_id
    ORDER BY nb DESC
""")
roles_per_actor.persist()  # Persist for multiple uses
roles_per_actor.show(5)

# 13. Descriptive Statistics of Roles per Actor
print("\nDescriptive Statistics of Roles per Actor:")
roles_per_actor_desc = roles_per_actor.describe('nb')
roles_per_actor_desc.show()

# 14. Plotting Roles per Actor (Log Scale)
roles_per_actor_collected = roles_per_actor.collect()
tabNb = [x.nb for x in roles_per_actor_collected]

plt.yscale('log')
plt.plot(tabNb)
plt.xlabel('actor')
plt.ylabel('nb roles')
plt.show()

# 15. Plotting Roles per Actor (Linear Scale)
plt.yscale('linear')
plt.plot(tabNb)
plt.xlabel('actor')
plt.ylabel('nb roles')
plt.show()

# 16. Number of Roles per Film Type (after 2010)
print("\nNumber of Roles per Film Type (after 2010):")
roles_per_film_type = spark.sql("""
    SELECT g.kind, count(*) as nb_roles
    FROM Cast_Info c
    JOIN Title f ON c.movie_id = f.id
    JOIN Kind_type g ON f.kind_id = g.id
    WHERE f.production_year > 2010
    GROUP BY g.kind
    ORDER BY nb_roles DESC
""")
roles_per_film_type.show(5)

# 17. Plotting Roles per Film Type
roles_per_film_type_collected = roles_per_film_type.collect()

tabNb = [x.nb_roles for x in roles_per_film_type_collected]
tabKind = [x.kind for x in roles_per_film_type_collected]

plt.yscale('log')
plt.bar(tabKind, tabNb)
plt.xlabel('kind')
plt.ylabel('nb roles')
plt.show()

# --- Further Analyses ---

# 18. Aggregation on Multiple Dimensions (Film Type, Production Year)
print("\nAggregation on Multiple Dimensions (Film Type, Production Year):")
multi_dim_agg = spark.sql("""
    SELECT k.kind, t.production_year, count(*) as nb_films
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    WHERE t.production_year IS NOT NULL
    GROUP BY k.kind, t.production_year
    ORDER BY k.kind, t.production_year DESC
""")
multi_dim_agg.show(10)

# 19. Hierarchical Dimensions (Roles - e.g., Actor, Director)
# The role_type table could represent a hierarchy of roles, but this is not explicitly
# defined in the schema.  Further analysis would require exploring the values in
# the role_type table to identify any implicit hierarchy.
print("\nHierarchical Dimensions (Roles):  See comments in code.")

# 20. CUBE Operator (Multiple Aggregations in One Query)
print("\nCUBE Operator (Multiple Aggregations in One Query):")
cube_query = spark.sql("""
    SELECT k.kind, t.production_year, count(*) AS nb_films
    FROM Title t
    JOIN Kind_Type k ON t.kind_id = k.id
    WHERE t.production_year IS NOT NULL
    GROUP BY k.kind, t.production_year WITH CUBE
    ORDER BY k.kind, t.production_year DESC
""")
cube_query.show(20)

# 21. Window Functions (Ranking Films by Year)
print("\nWindow Functions (Ranking Films by Year):")
window_spec = Window.partitionBy("production_year").orderBy(desc("nb_films"))

ranked_films = films_per_year.withColumn("rank", rank().over(window_spec))
ranked_films.show(10)

# 22. Window Functions (Aggregate over a Sliding Window - not really applicable without time series data)
# This is difficult to illustrate meaningfully with this dataset, as there isn't a natural
# time series aspect beyond "production_year."
print("\nWindow Functions (Aggregate over a Sliding Window):  See comments in code.")

# 23. Data Denormalization (Movie with List of Actors)
print("\nData Denormalization (Movie with List of Actors):")
# Note: This query may be slow and memory-intensive for large datasets.
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

# 24. Reusing Queries with Temp Views
print("\nReusing Queries with Temp Views:")
films_per_type.createOrReplaceTempView("FilmsPerType")
top_film_types = spark.sql("""
    SELECT kind_id, nb
    FROM FilmsPerType
    WHERE nb > 100  -- Example filter
    ORDER BY nb DESC
""")
top_film_types.show(10)