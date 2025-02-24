from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_unixtime, rank, desc, collect_list, size, sum, udf, col, expr
from pyspark.sql.types import DateType, DoubleType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

# Initialize Spark session
spark = SparkSession.builder.appName("VKAnalysis").getOrCreate()

# Set the path to the data
path = "/dataset/VKRU18s/"
vk_file = path + "vk_001.json"

# Load the data, remove duplicates, and calculate total records
data = spark.read.format("json").load(vk_file).dropDuplicates()
total = data.count()

#------------------------------------------------------------------------------
# Check which attributs is optionnal and which are obligatory
#------------------------------------------------------------------------------

def optional_vs_obligatory():
    """Calculates and displays the frequency of optional attributes."""

    attrs = ["event","event.event_id","event.event_id.post_id","event.event_id.post_owner_id","event.event_id.comment_id","event.event_id.shared_post_id","event.author","event.attachments","event.geo","event.tags","event.creation_time"]
    attrs_freq = [data.where(col(x).isNotNull()).count() for x in attrs]
    print('Frequence of each columns:')
    for i in range(len(attrs)):
        print("Frequency of " + attrs[i] + ' is: ' + str(attrs_freq[i]))

    #Checking whether an attribute that the event_id exists only if the event type is comment

    comment_check = data.where("event.event_type='comment'").where("event.event_id.comment_id is null").count()
    print('Checking that if the event type is comment the comment ID must not be null : ' +str(comment_check))
    shared_check = data.where("event.event_type='share'").where("event.event_id.shared_post_id is null").count()
    print('Checking that if the event type is shared_post the shared_post_id must not be null : ' +str(shared_check))

# Print the data first 20 rows
data.show()

# Print the schema
data.printSchema()

#------------------------------------------------------------------------------
# Count and display the total
#------------------------------------------------------------------------------

print('Total number of records')
print(total)

#------------------------------------------------------------------------------
# Count unique post IDs
#------------------------------------------------------------------------------

unique_posts = data.select("event.event_id.post_id").distinct().count()
print('Number of unique posts is :' + str(unique_posts))

#------------------------------------------------------------------------------
# Compute Posts count per event
#------------------------------------------------------------------------------

postPerType = data.select("event.event_type","event.event_id.post_id").distinct().groupBy("event_type").count()

print('Posts counts per event type: ')
postPerType.show()

#------------------------------------------------------------------------------
# Flatten the list of Tags
#------------------------------------------------------------------------------

# Explode flattens the tag array in the 'data' table to explode it for the dataWithTags
dataWithTags = data.withColumn("tag", explode(col("event.tags")))
dataWithTags.show()

#------------------------------------------------------------------------------
# Compute the number of posts per tag
#------------------------------------------------------------------------------

# group data to make calculate the number of distinct posts per tag
postsPerTag = dataWithTags.groupBy("tag").count().orderBy(col("count").desc())
print('Number of posts per tag: ')
postsPerTag.show()

#------------------------------------------------------------------------------
# Compute the number of distinct authors per tag
#------------------------------------------------------------------------------

authCountPerTag =dataWithTags.groupBy("tag").agg(expr("count(_id)").alias("nbAuths"))
authCountPerTag.orderBy(col("nbAuths").desc).show()

#------------------------------------------------------------------------------
# Fact checking Using wikipedia data
#------------------------------------------------------------------------------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define the schema for the votes
schema = StructType([
    StructField("name", StringType(), True),
    StructField("party", StringType(), True),
    StructField("votes", LongType(), True)
])

# Create the data
data2 = [
    Row("putin", "Independent", 56430712),
    Row("grudinin", "Communist", 8659206),
    Row("zhirinovsky","Liberal Democratic Party",4154985),
    Row("sobchak","Civic Initiative",1238031),
    Row("yavlinsky","Yabloko",769644),
    Row("titov","Party of Growth",556801)
]

votes = spark.createDataFrame(data2, schema)
votes.printSchema()
votes.show()

#------------------------------------------------------------------------------
# Generate and Display the number of Votes per number of Authors to compare between the Wikipedia data with the Tags extracted
#------------------------------------------------------------------------------

# Merge the author counts into the votes
votesCount = votes.join(authCountPerTag,votes["name"] == authCountPerTag["tag"]).orderBy(col("votes").desc).select("name","votes","nbAuths")
votesCount.show()

# Add the ranks obtained with the help of number of votes and those obtained with the help of numbers of authors. What do you notice?
# The goal is to create a cube to aggregate the posts on three dimensions; tag, type of event and the month it was created.
# Now what we do is get help from function from_unixtime

from pyspark.sql.functions import col, from_unixtime

windowSpec = Window.orderBy(desc("votes"))

# Adding ranks and checking
ranked = votesCount.withColumn("votes_rank",rank().over(Window.orderBy(col("votes").desc()))).withColumn("nbAuths_rank",rank().over(Window.orderBy(col("nbAuths").desc())))
ranked.show()

#------------------------------------------------------------------------------
# Add a 'month' attribute
#------------------------------------------------------------------------------

dataTagMon = dataWithTags.withColumn("month",from_unixtime(col("event.creation_time"),"M"))
dataTagMon.show()

#Rollup aggregates data at multiple levels, from the most detailed to the most summarized, based on the specified columns
cub_ev_tag_mo =  dataTagMon.rollup("event.event_type", "tag", "month").count()
cub_ev_tag_mo_notnull = cub_ev_tag_mo.where("event_type is not null and tag is not null and month is not null")
cub_ev_tag_mo_notnull.orderBy(col("count").desc).show()

#What this step does it does calculate the number of every event_type, tag and month

#------------------------------------------------------------------------------
# Create a cross table
#------------------------------------------------------------------------------
#Group the counts based on event type and the month, to create counts for all dimensions in that group

monthEvent = cub_ev_tag_mo_notnull.groupBy("event_type","month")
monthEvent.printSchema
monthEvent.show

#Show the pivoted table to indicate a co-relation between the counts per mont
monthEvent = cub_ev_tag_mo_notnull.groupBy("month").pivot("event_type").sum("count")
monthEvent.show()

#------------------------------------------------------------------------------
# Co-occurrence Matrix of Tags
#------------------------------------------------------------------------------

# Get all tags used by a single author
authTag = dataWithTags.select("event.author.id","tag").distinct()

# Join tag author with the tags and rename a column
dataWithTags_a = dataWithTags.withColumnRenamed("event", "event1").withColumnRenamed("tag", "otherTag")

# get the data from the tags
TupletagsPerAuthor_a = authTag.join(dataWithTags_a,authTag.tag == dataWithTags_a.otherTag).where(f"authTag.id = dataWithTags_a.event1.author.id and tag > otherTag")

# Now group by the author, the number of posts with those tags and show the number of occurences and name it as count
tagCoOcc = TupletagsPerAuthor_a.groupBy("tag", "otherTag").agg(size(collect_list(authTag.col("event1.author.id"))).alias("count"))
tagCoOcc.show(30)

# Now group by the number of other tag and take the new sum for each one, and name it count
tagMat = tagCoOcc.groupBy("tag").pivot("otherTag").agg(sum("count").alias("count"))
tagMat.show()

# Stop Spark session
spark.stop()