# -*- coding: utf-8 -*-
"""
Social Network Analysis with Spark GraphX (Scala)

This project demonstrates graph analysis techniques on a social network dataset
using Spark GraphX.

Original file: spark_graphx.ipynb
"""

# The following code is in Scala and uses the Spark GraphX API.

# Import necessary libraries (These will be translated to Scala)
"""
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
sc.setLogLevel("ERROR") // Suppress excessive logging
"""

# --- Data Loading and Graph Creation ---
# Define the directory containing the dataset
"""
val DATASET_DIR="dataset/"
"""

# Load the edge data (relationships between users)
"""
var file = "facebook_edges_prop.csv"
var lines = sc.textFile(DATASET_DIR+file, 4)
"""

# Create the edges RDD
"""
val edgesList:RDD[Edge[(String, Int)]] = lines.map{s=>
    val parts = s.split(",")
    Edge(parts(0).toLong, parts(1).toLong, (parts(2), parts(3).toInt))
}.distinct()
"""
# Explanation:
# - Reads each line from the file.
# - Splits each line into parts based on the comma delimiter.
# - Creates an Edge object, where:
#   - Source vertex ID: parts(0).toLong
#   - Destination vertex ID: parts(1).toLong
#   - Edge attribute: A tuple containing (relationship type (String), weight (Int))
# - Applies distinct() to remove duplicate edges.

# Load the vertex data (user properties)
"""
file = "facebook_users_prop.csv"
lines = sc.textFile(DATASET_DIR+file, 4)
"""

# Create the vertices RDD
"""
val vertexList:RDD[(VertexId,(String, String, Int))] = lines.map{s=>
    val parts = s.split(",")
    (parts(0).toLong, (parts(1), parts(2), parts(3).toInt))
}.distinct()
"""
# Explanation:
# - Reads each line from the file.
# - Splits each line into parts based on the comma delimiter.
# - Creates a tuple (Vertex ID, (property1, property2, property3)), where:
#   - Vertex ID: parts(0).toLong
#   - User attributes: A tuple containing (name (String), location (String), age (Int))
# - Applies distinct() to remove duplicate vertices.

# Create the graph
"""
val graph = Graph.apply(vertexList, edgesList)
"""
# Explanation:
# - Creates a Graph object from the vertices and edges RDDs.

# --- Basic Graph Information ---
# Print the number of edges
"""
graph.numEdges
"""

# Print the number of vertices
"""
graph.numVertices
"""

# Count the number of triplets (source vertex, edge, destination vertex)
"""
graph.triplets.count
"""

# --- Graph Queries ---
# Exercice 2
# Filter and print users named "Kendall"

"""
graph.vertices.filter{case(id,(p,n,a)) => p=="Kendall"}.collect.foreach(u=>println(u._1+" "+u._2._2+" "+u._2._3))
"""
# Explanation:
# - Filters the vertices RDD to keep only vertices where the first attribute (name) is "Kendall".
# - Collects the filtered vertices and prints their ID, location, and age.

# Exercice 3
# Find the first name of any user connected to a user named "Kendall"

"""
graph.triplets.filter{t =>t.srcAttr._1 =="Kendall" || t.dstAttr._1 =="Kendall"}.map(t=>if(t.srcAttr._1=="Kendall") t.dstAttr._1 else t.srcAttr._1).collect.foreach(u=>println(u))
"""
# Explanation:
# - Filters the triplets RDD to keep only triplets where either the source or destination vertex has the name "Kendall".
# - Maps the filtered triplets to extract the name of the other vertex (the one that's not "Kendall").
# - Collects the names and prints them.

# Exercice 4
# Display the name of the attributes (first) of the first element

"""
graph.triplets.first.srcAttr._1
"""
# Get source atributes from the first row to display to user

# Exercice 4
# Print the IDs of the users who liked Kendall with more than 70 (the one who are colleague)

"""
graph.triplets.filter{t=>t.dstAttr._1 =="Kendall" & t.attr._1=="colleague" & t.attr._2>70}.map(t=> t.srcId).distinct().collect().take(10).foreach(x=>println(x))
"""
# Explanation:
# - Filters the triplets RDD to keep only triplets where:
#   - The destination vertex (user liked) has the name "Kendall".
#   - The relationship type is "colleague".
#   - The relationship weight is greater than 70.
# - Extracts the IDs of the source vertices (users who liked Kendall).
# - Applies distinct() to remove duplicate user IDs.
# - Collects the first 10 distinct user IDs and prints them.

# Exercice 5
# Id from a destiny

"""
graph.edges.first.dstId
"""
# Display the destination id of the attribute

# Exercice 5
# The user who has more than 80 friends

"""
graph.edges.filter{t=>t.attr._1=="friend" & t.attr._2>80}.map(t=>(t.srcId,1)).reduceByKey((a,b)=>a+b).collect.maxBy(_._2)
"""
# Explanation:
# - Filters the edges RDD to keep only edges where the relationship type is "friend" and the weight is greater than 80.
# - Maps the filtered edges to create pairs of (source vertex ID, 1).
# - Reduces the pairs by key (source vertex ID) to count the number of friends for each user.
# - Collects the results and finds the user with the maximum number of friends.

# Exercice 6
# What are the atributes for the people name is Kendall

"""
graph.triplets
    .filter{t=>t.dstAttr._1=="Kendall" ||t.srcAttr._1=="Kendall" }
    .map(t=>(t.srcAttr,t.dstAttr)).foreach{case ((p,n,a), (p1,n1,a1)) => if(p=="Kendall") println(p1+" "+n1+" "+a1)
else println(p+" "+n+" "+a)}
"""
# Display all the atributes to a person whom name is Kendall

"""
graph.collectNeighbors(EdgeDirection.Either).innerJoin(graph.vertices.filter(t=>t._2._1=="Kendall")){
case (id, list, attrs) => list}.values.collect.foreach(t=>println(t.size))
"""

"""
graph.aggregateMessages[Array[String]] (
triplet => {
if(triplet.srcAttr._1=="Kendall")
triplet.sendToSrc(Array("1"))
if(triplet.dstAttr._1=="Kendall")
triplet.sendToDst(Array("1"))
},
(a,b) => b++a
).values.collect.foreach(t=>println(t.size))
"""

# Exercice 7
# Which one has the least followers

"""
var minDeg =graph.inDegrees.values.reduce((a,b)=> if(a<b) a else b)
graph.inDegrees.filter(t=>t._2==minDeg).innerJoin(graph.vertices){case (id, d, u) => u._1}.
collect.foreach(x=>println(x._1 + " Name "+" "+x._2))
"""

# Exercice 8
# Name indegree which don't have a follower
# Name indegree 0

"""
graph.outerJoinVertices(graph.inDegrees){
case (id, prop, opt) => (prop._1,opt.getOrElse(0))}.vertices.filter(t=> t._2._2==0).collect.foreach(x=>println(x._2._1 + " indegree "+" "+x._2._2))
"""

# Exercice 9
# Name five followers and five friends

"""
graph.outerJoinVertices(graph.inDegrees){
case (id, prop, opt) => (prop._1,opt.getOrElse(0))
}.outerJoinVertices(graph.outDegrees){
case (id, prop, opt) => (prop._1,prop._2,opt.getOrElse(0))
}.vertices.filter(v=>v._2._2 == 5 && v._2._3 == 5).collect.foreach(x=>println(x._2._1+" "+x._2._2))
"""

# Exercice 10
# Who is olders

"""
val oldest = graph.aggregateMessages[(String, Int)](
triplet => { // Map Function
if(List("Dalvin", "Kendall", "Elena").contains(triplet.dstAttr._1) &&
triplet.attr._1 == "friend")
// Send message to destination vertex containing counter and age
triplet.sendToDst((triplet.srcAttr._1,triplet.srcAttr._3))
},
(a,b) => if(a._2 > b._2) a else b
)

graph.vertices.filter{t=>List("Dalvin", "Kendall", "Elena").contains(t._2._1)}.leftJoin(oldest)
{ (id, user, optOldestFollower) =>
optOldestFollower match {
case None => s"${user._1} does not have any followers."
case Some((name, age)) => s"${name} aged ${age} is the oldest follower of ${user._1}."
}
}.collect.foreach { case (id, str) => println(str) }
"""

# Exercice 11
# Count the most friends

"""
val com = graph.aggregateMessages[Int] (
triplet =>{
if(triplet.dstAttr._1== "Kendall" || triplet.dstAttr._1== "Lilia" && triplet.attr._1 == "friend")
triplet.sendToSrc(1)
},
(a,b) => a+b
)
com.filter{case(id, nb) => nb > 1}.collect.foreach(u=>println(u._1))
"""

# Exercice 12
# Average Age

"""
val ages = graph.aggregateMessages[(Int, Double)]( //on obtient un nouveau graphe (ages)
triplet => { // Map Function
    //if(triplet.attr._1=="friend")
    triplet.sendToSrc(1,triplet.dstAttr._3)
    triplet.sendToDst(1,triplet.srcAttr._3)
},
(a, b) => (a._1 + b._1, a._2 + b._2)
)
ages.mapValues(t=>t._2/t._1).collect.foreach(println(_))
"""

# Exercice 13
# Tags that each user has

"""
val tarcs = graph.aggregateMessages[Set[String]](
triplet => {
    triplet.sendToSrc(Set(triplet.attr._1))
},
(a,b) => a ++ b
)
graph.outerJoinVertices(tarcs){(id, oldAttr, typeOpt) =>
typeOpt match{
case Some(typeOpt) => (oldAttr._1,typeOpt)
case None => (oldAttr._1,Set())
}
}.vertices.filter{case (id, (p, l))=>p == "Elena"}.collect.foreach(println(_))
"""

# Exercice 14
# Find the neightbours for a user for whom name is Deana, or where relationship is Friend

"""
val neighs = graph.aggregateMessages[Boolean] (
triplet => {
if(triplet.srcAttr._1=="Deana" && triplet.attr._1=="friend")
triplet.sendToDst(true)
},
(a,b) => a
)

graph.outerJoinVertices(neighs){(id, oldAttr, isNeigh) =>
isNeigh match{
case Some(isNeigh) => (oldAttr._1,isNeigh)
case None => (oldAttr._1,false)
}}.triplets.filter{t=>t.srcAttr._2 == true && t.attr._1 == "friend"}.
map(t=>t.dstAttr._1).collect.foreach(println(_))
"""

# Exercice 15
# Find the distance between the users and user named Deana

"""
val initialGraph = graph.mapVertices( (id, attr) => if (attr._1 == "Deana") (0.0, attr._1) else
(Double.NegativeInfinity, attr._1))
val nn = initialGraph.pregel(Double.NegativeInfinity)(
(id, dist, newDist) => (Math.max(dist._1,newDist), dist._2),
triplet => {
if(triplet.srcAttr._1>triplet.dstAttr._1)
Iterator((triplet.dstId, triplet.srcAttr._1+1.0))
else Iterator.empty
},
(a,b) => math.max(a,b)
)
//nn.vertices.filter{v=>v._2._1 > 1.0 }.collect.foreach(println(_))

nn.vertices.filter{v=>v._2._1 > 0 }.collect.foreach(println(_))

nn.vertices.map{v=>v._2._1 }.collect.foreach(println(_))
"""