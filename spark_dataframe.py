from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, collect_list, udf
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("BooksDataAnalysis").getOrCreate()

# Define data path
path = "/dataset/Books/"

# Load datasets
users_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "users.csv")
books_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "books.csv")
ratings_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "ratings.csv")

#------------------------------------------------------------------------------
# Simple Queries
#------------------------------------------------------------------------------

# Identifiers of users from 'france'
s0 = users_df.where("country=\"france\"")

# Identifiers of books published in 2000
s1 = books_df.where("year=2000")

# Identifiers of books evaluated with a rating > 3
s2 = ratings_df.where("rating>3")

#------------------------------------------------------------------------------
# Aggregation Queries
#------------------------------------------------------------------------------

# Number of users per country with decreasing sort
q1 = users_df.groupBy("country").count().sort(desc("count"))

# Country with the highest number of users (no ex aequo)
q2 = q1.limit(1)

# Year with the highest number of books published (no ex aequo)
q3 = books_df.groupBy("year").count().sort(desc("count")).limit(1)

# Publishers having published more than 10 books in total
q4 = books_df.groupBy("publisher").count().where("count>10").sort(desc("count"))

# Publishers having published more than 5 books each year they have published a book
q5 = books_df.groupBy("year","publisher").count().where("count>5")

# The average rating per book
q6 = ratings_df.groupBy("bookid").agg(F.avg("rating"))

# The number of books having been rated 5
q7 = ratings_df.where("rating=5").count()

# The identifier of the user having rated the highest number of books. No ex aequo
q8 = ratings_df.groupBy("userid").count().sort(desc("count")).limit(1)

#------------------------------------------------------------------------------
# Join Queries
#------------------------------------------------------------------------------

# The publishers of the books rated by users living in France
q9 = users_df.join(ratings_df, users_df.userid == ratings_df.userid).filter(users_df.country == "france").join(books_df,ratings_df.bookid == books_df.bookid).select(books_df.publisher).distinct()

# The publishers of the books that haven't been rated by users living in France
q10 = books_df.join(ratings_df,books_df.bookid == ratings_df.bookid,"leftanti").join(users_df,ratings_df.userid == users_df.userid,"leftanti").select(books_df.publisher).distinct()

# For each country, the average number of books with a rating > 3 given by users from this country

q11 = users_df.join(ratings_df, users_df.userid == ratings_df.userid).filter(ratings_df.rating > 3).groupBy(users_df.country).agg(F.avg(ratings_df.bookid))

# For each book, the average age of the users having rated it
q12 = ratings_df.join(users_df,ratings_df.userid == users_df.userid).groupBy(ratings_df.bookid).agg(F.avg(users_df.age))

# Pairs of users having rated the same number of books
q13 = ratings_df.groupBy("userid").count().withColumnRenamed("count","nb_rated").alias("q1")\
          .join(ratings_df.groupBy("userid").count().withColumnRenamed("count","nb_rated").alias("q2"),F.col("q1.nb_rated")==F.col("q2.nb_rated"))\
          .select(F.col("q1.userid"),F.col("q2.userid"),F.col("q1.nb_rated")).filter(F.col("q1.userid")!=F.col("q2.userid"))

#------------------------------------------------------------------------------
# User Defined Functions
#------------------------------------------------------------------------------

# Define UDFs (User-Defined Functions)
def common_books(list1, list2):
    """Calculates the cardinality of the intersection of two sets."""
    set1 = set(list1)
    set2 = set(list2)
    return len(set1.intersection(set2))

def union_books(list1, list2):
    """Calculates the cardinality of the union of two sets."""
    set1 = set(list1)
    set2 = set(list2)
    return len(set1.union(set2))

# Register UDFs with Spark
commonBooks = udf(common_books, DoubleType())
unionBooks = udf(union_books, DoubleType())

# Pairs of users with the number of common books they have rated
q14a = ratings_df.groupBy("userid").agg(collect_list("bookid").alias("books1")).withColumnRenamed("userid","uid1")
q14b = q14a.crossJoin(q14a.withColumnRenamed("uid1","uid2").withColumnRenamed("books1","books2"))

# Pairs of users with the Jaccard similarity calculated on the books they have rated
q15 = q14b.withColumn("common", commonBooks(F.col("books1"),F.col("books2"))) \
    .withColumn("union", unionBooks(F.col("books1"), F.col("books2"))) \
    .withColumn("jaccard", F.col("common") / F.col("union"))

#Print all the queries from s0 to q15
print('users living in France', s0.show())
print('Books Published in 2000', s1.show())
print('ratings over 3', s2.show())
print('user counts by country', q1.show())
print('Country with the largest number of users', q2.show())
print('Year with highest number of books', q3.show())
print('Publishers with more than 10 books', q4.show())
print('Publisher with more than 5 books/year', q5.show())
print('Average rating per book', q6.show())
print('Number of books rated as 5', q7)
print('The identifier of the user having rated the most books', q8.show())
print('Publishers in France', q9.show())
print('Publishers not from France', q10.show())
print('Average ratings per country for ratings > 3', q11.show())
print('Average user ages per book', q12.show())
print('Pairs of users rating the same # of books', q13.show())
print('Jaccard Similarities', q15.show())

# Stop Spark session
spark.stop()