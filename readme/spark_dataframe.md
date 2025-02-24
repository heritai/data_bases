# Book Data Analysis with Spark DataFrames

This project demonstrates how to analyze a book dataset using Spark DataFrames. It covers data loading, transformation, aggregation, and join operations to extract valuable insights.

## Project Overview

This project aims to analyze the "Books" dataset, which includes information about books, users, and their ratings. The analysis uses Spark DataFrames to perform the following tasks:

*   **Data Loading and Preparation:** Load data from CSV files into Spark DataFrames.
*   **Basic Queries:** Extract subsets of data based on specific criteria.
*   **Aggregation Queries:** Calculate summary statistics and group data based on different attributes.
*   **Join Queries:** Combine data from multiple DataFrames to explore relationships between entities.
*   **User-Defined Functions (UDFs):** Create custom functions to perform advanced calculations.

## Data Sources

The project uses the following CSV files:

*   **books.csv:** Contains information about books (bookid, titlewords, authorwords, year, publisher).
*   **users.csv:** Contains information about users (userid, country, age).
*   **ratings.csv:** Contains information about user ratings of books (userid, bookid, rating).

## Code Structure

The `spark_dataframe.py` script is organized as follows:

1.  **Spark Session Initialization:** Initializes a SparkSession.
2.  **Data Loading:** Loads the data from CSV files into Spark DataFrames.
3.  **Basic Queries:** Implements simple queries to filter data.
4.  **Aggregation Queries:** Implements aggregation queries to calculate summary statistics.
5.  **Join Queries:** Implements queries that join multiple DataFrames.
6.  **User-Defined Functions:** Implements and registers UDFs for custom calculations.

## Data Analysis

The project performs a variety of data analysis tasks using Spark DataFrames.

### 1. Data Loading and Preparation

The data loading process involves reading the CSV files into Spark DataFrames and printing the schemas to verify the data types:

```python
# Initialize Spark session
spark = SparkSession.builder.appName("BooksDataAnalysis").getOrCreate()

# Define data path
path = "dataset/Books/"

# Load datasets
users_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "users.csv")
books_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "books.csv")
ratings_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "ratings.csv")
```

### 2. Simple Queries

These queries filter the DataFrames based on specific conditions:

*   **Users from France:**

    ```python
    s0 = users_df.where("country=\"france\"")
    ```

*   **Books Published in 2000:**

    ```python
    s1 = books_df.where("year=2000")
    ```

*   **Books with Ratings Greater Than 3:**

    ```python
    s2 = ratings_df.where("rating>3")
    ```

### 3. Aggregation Queries

These queries calculate summary statistics and group data based on different attributes:

*   **Number of Users per Country:**

    ```python
    q1 = users_df.groupBy("country").count().sort(desc("count"))
    ```

*   **Country with the Highest Number of Users:**

    ```python
    q2 = q1.limit(1)
    ```

*   **Year with the Highest Number of Books Published:**

    ```python
    q3 = books_df.groupBy("year").count().sort(desc("count")).limit(1)
    ```

*   **Publishers with More Than 10 Books:**

    ```python
    q4 = books_df.groupBy("publisher").count().where("count>10").sort(desc("count"))
    ```
*   **Publishers with More Than 5 Books Each Year:**
    ```python
    q5 = books_df.groupBy("year","publisher").count().where("count>5")
    ```

### 4. Join Queries

These queries combine data from multiple DataFrames to explore relationships between entities:

*   **Publishers of Books Rated by Users Living in France:**

    ```python
    q9 = users_df.join(ratings_df, users_df.userid == ratings_df.userid).filter(users_df.country == "france").join(books_df,ratings_df.bookid == books_df.bookid).select(books_df.publisher).distinct()
    ```
    This query joins the books to ratings, the ratings with user and then takes only the values related to the french users then display only the book id.

*   **Publishers of Books Not Rated by Users Living in France:**

    ```python
    q10 = books_df.join(ratings_df,books_df.bookid == ratings_df.bookid,"leftanti").join(users_df,ratings_df.userid == users_df.userid,"leftanti").select(books_df.publisher).distinct()
    ```

    Here, instead of selecting the data from the french user, this query is looking for the data that is not from the french users. This time, `leftanti` will bring the values not shared with the joined table.

*   **Average Number of Highly Rated Books per Country:**

    ```python
    q11 = users_df.join(ratings_df, users_df.userid == ratings_df.userid).filter(ratings_df.rating > 3).groupBy(users_df.country).agg(F.avg(ratings_df.bookid))
    ```
    This joins the users and ratings data, filters it by the data where the rating is > 3, and groups the data by the country then aggregates the avg.

*   **Average Age of Users Who Rated Each Book:**

    ```python
    q12 = ratings_df.join(users_df,ratings_df.userid == users_df.userid).groupBy(ratings_df.bookid).agg(F.avg(users_df.age))
    ```

    This query calculates the average age for the books. It joins all data on all rating data for a user, and then aggregates them.

*   **Pairs of Users Who Have Rated The Same Number of Books:**
    ```python
    q13 = ratings_df.groupBy("userid").count().withColumnRenamed("count","nb_rated").alias("q1")\
        .join(ratings_df.groupBy("userid").count().withColumnRenamed("count","nb_rated").alias("q2"),F.col("q1.nb_rated")==F.col("q2.nb_rated"))\
        .select(F.col("q1.userid"),F.col("q2.userid"),F.col("q1.nb_rated")).filter(F.col("q1.userid")!=F.col("q2.userid"))
    ```

    This query counts the number of ratings for each user, then joins with the total, so if those totals align, it means these are the users who have similar numbers of book ratings. Finally the not equal is there to make sure we don't get the same user compared with itself.

### 5. User-Defined Functions (UDFs)

The project demonstrates how to create and use UDFs to perform custom calculations.

*   **`common_books(list1, list2)`:** Calculates the number of common books between two users.
*   **`union_books(list1, list2)`:** Calculates the total number of unique books rated by two users.

The UDFs are registered with Spark:

```python
commonBooks = udf(common_books, DoubleType())
unionBooks = udf(union_books, DoubleType())
```

These functions are used to calculate the Jaccard index, which measures the similarity between users based on the books they have rated:

```python
# Pair the users based on their interactions with the book id
q14a = ratings_df.groupBy("userid").agg(collect_list("bookid").alias("books1")).withColumnRenamed("userid","uid1")
# create the cartesian product
q14b = q14a.crossJoin(q14a.withColumnRenamed("uid1","uid2").withColumnRenamed("books1","books2"))

# use the previous functions to calculate Jaccard similarity
q15 = q14b.withColumn("common", commonBooks(F.col("books1"),F.col("books2"))) \
    .withColumn("union", unionBooks(F.col("books1"), F.col("books2"))) \
    .withColumn("jaccard", F.col("common") / F.col("union"))
```

## Conclusion

This project demonstrates a comprehensive approach to data analysis with Spark DataFrames. It covers a wide range of operations, from basic data loading and filtering to more advanced techniques like aggregation, joins, and UDFs. The techniques and code provided here can be used as a starting point for analyzing other datasets and developing custom data analysis pipelines.
