# Social Network Analysis with Spark GraphX (Scala)

This project demonstrates graph analysis techniques on a social network dataset using Spark GraphX.

## Project Overview

The project analyzes a Facebook-like social network dataset using Spark GraphX, a powerful graph processing framework. The main goals are to:

1.  **Graph Creation:** Construct a graph from edge and vertex data.
2.  **Basic Graph Analysis:** Extract basic information about the graph (number of vertices, edges, etc.).
3.  **Advanced Graph Queries:** Use GraphX API to query the graph and extract insights about users and their relationships.

## Data Sources

The project uses the following data sources:

*   **facebook_edges_prop.csv:** Contains edge data, representing relationships between users.
    *   Columns: `source vertex ID`, `destination vertex ID`, `relationship type`, `weight`
*   **facebook_users_prop.csv:** Contains vertex data, representing user properties.
    *   Columns: `vertex ID`, `name`, `location`, `age`

The dataset should be placed in the `/dataset` directory within the configured `DATASET_DIR`.

## Core Components

### 1. Graph Creation

The project begins by loading the edge and vertex data and creating a GraphX graph:

```scala
val DATASET_DIR="dataset/"

var file = "facebook_edges_prop.csv"
var lines = sc.textFile(DATASET_DIR+file, 4)

val edgesList:RDD[Edge[(String, Int)]] = lines.map{s=>
    val parts = s.split(",")
    Edge(parts(0).toLong, parts(1).toLong, (parts(2), parts(3).toInt))
}.distinct()

file = "facebook_users_prop.csv"
lines = sc.textFile(DATASET_DIR+file, 4)

val vertexList:RDD[(VertexId,(String, String, Int))] = lines.map{s=>
    val parts = s.split(",")
    (parts(0).toLong, (parts(1), parts(2), parts(3).toInt))
}.distinct()

val graph = Graph.apply(vertexList, edgesList)
```

*   The code loads the edge and vertex data from CSV files into RDDs.
*   The `Edge` and vertex tuples are created.
*   The `Graph.apply()` method creates a GraphX graph from the vertex and edge RDDs.

### 2. Graph Queries

The project then demonstrates a series of graph queries using the GraphX API.

*   **Filter Users by Name:**

    ```scala
    graph.vertices.filter{case(id,(p,n,a)) => p=="Kendall"}.collect.foreach(u=>println(u._1+" "+u._2._2+" "+u._2._3))
    ```

    This code filters the vertices RDD to find users named "Kendall" and prints their information.

*   **Find Neighbors:**

    ```scala
    graph.triplets.filter{t =>t.srcAttr._1 =="Kendall" || t.dstAttr._1 =="Kendall"}.map(t=>if(t.srcAttr._1=="Kendall") t.dstAttr._1 else t.srcAttr._1).collect.foreach(u=>println(u))
    ```

    This code finds the names of the users who are either source or destination of the given vertices.

*   **Users Who Like "Kendall" with a High Weight:**

    ```scala
    graph.triplets.filter{t=>t.dstAttr._1 =="Kendall" & t.attr._1=="colleague" & t.attr._2>70}.map(t=> t.srcId).distinct().collect().take(10).foreach(x=>println(x))
    ```

    This code finds users who have a "colleague" relationship with "Kendall" and the relationships weight is above 70.

*   **User with the Most Friends:**

    ```scala
    graph.edges.filter{t=>t.attr._1=="friend" & t.attr._2>80}.map(t=>(t.srcId,1)).reduceByKey((a,b)=>a+b).collect.maxBy(_._2)
    ```

    This example demonstrates:
    *   Filtering edges to include "friend" relationships with a weight greater than 80.
    *   Mapping edges to (source vertex ID, 1) pairs.
    *   Reducing by key to count the number of friends for each user.
    *   Finding the user with the maximum friend count.

*   **Outer Join with In-Degrees:**

    ```scala
    graph.outerJoinVertices(graph.inDegrees){
    case (id, prop, opt) => (prop._1,opt.getOrElse(0))}.vertices.filter(t=> t._2._2==0).collect.foreach(x=>println(x._2._1 + " indegree "+" "+x._2._2))
    ```

    This will calculate the in degrees for every user on the graph

*   **Combining inDegrees and outDegrees**

    ```scala
    graph.outerJoinVertices(graph.inDegrees){
    case (id, prop, opt) => (prop._1,opt.getOrElse(0))
    }.outerJoinVertices(graph.outDegrees){
    case (id, prop, opt) => (prop._1,prop._2,opt.getOrElse(0))
    }.vertices.filter(v=>v._2._2 == 5 && v._2._3 == 5).collect.foreach(x=>println(x._2._1+" "+x._2._2))
    ```
   Here we perform a chain of operations that show the graph data together with other atributes.

*   **Find oldest followers and the distance to Deana**
    ```scala
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
    ```
    The point from the above algorithm is that, given some condition we want to aggregate, in here what is done is that we can find what other atributes can the given users have on them. The `max`, `min` average can be useful to understand those patterns.

*   **Aggregate Message**
    ```scala
    graph.aggregateMessages[Int] (
    triplet =>{
    if(triplet.dstAttr._1== "Kendall" || triplet.dstAttr._1== "Lilia" && triplet.attr._1 == "friend")
    triplet.sendToSrc(1)
    },
    (a,b) => a+b
    )
    com.filter{case(id, nb) => nb > 1}.collect.foreach(u=>println(u._1))
    ```

    Using the aggregate message will help us show what is the conditions to validate or aggregate the messages together

## Next Steps

*   Further explore graph algorithms available in GraphX.
*   Experiment with different datasets and graph structures.
*   Optimize the code for performance by tuning Spark configuration parameters.

This project provides a practical introduction to graph analysis with Spark GraphX, demonstrating key techniques for building, querying, and analyzing social networks.
