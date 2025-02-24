# Databases Repository

Welcome to the `data_bases` repository! This collection showcases various data analysis projects, primarily focused on utilizing:

*   **Relational Databases and Spark:** Analyzing structured datasets using PySpark and the DataFrame API, providing SQL-like querying and scalable data manipulation.
*   **Graph Databases:** Exploring graph-structured data with Neo4j, leveraging its native graph model and Cypher query language to uncover relationships and patterns.
*   **Spark Graph Processing:** Implementing graph algorithms and analyses with Apache Spark's GraphX and RDD APIs for large-scale graph analytics.

## Overview

This repository serves as a resource for demonstrating practical applications of data analysis and graph database technologies. Each project tackles a specific dataset and utilizes different tools to extract meaningful insights. The emphasis is on:

*   **Data Exploration:** Understanding the characteristics and structure of various datasets.
*   **Data Wrangling:** Cleaning, transforming, and preparing data for analysis.
*   **Data Analysis:** Applying analytical techniques to answer specific questions and identify trends.
*   **Visualization:** Presenting findings using visualizations where applicable.

## Technologies Used

*   **Apache Spark:** A powerful open-source distributed processing system used for big data processing and analytics.
    *   **PySpark:** Spark's Python API, providing DataFrame and SQL capabilities.
    *   **Spark RDDs:** Spark's Resilient Distributed Datasets, a fundamental data abstraction for distributed computing.
    *   **Spark GraphX:** Spark's API for graph processing.
*   **Neo4j:** A graph database management system that stores data in a network of nodes and relationships.
    *   **Cypher:** Neo4j's declarative graph query language.
*   **Python:** A versatile programming language widely used for data analysis and scripting.
    *   **pandas:** A Python data analysis library for data manipulation and analysis.
    *   **Matplotlib:** A Python plotting library for creating visualizations.

## Project Descriptions

The following table provides a brief description of each project and a link to its detailed documentation:

| File Name                  | Project Description                                                                                                                                                            | Documentation Location                                                  |
| :------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------- |
| `pyspark_imdb.py`          | Explores the IMDB dataset using PySpark DataFrames to analyze movie information, cast details, and relationships between entities.                                                     | [readme/pyspark_imdb.md](readme/pyspark_imdb.md)                       |
| `scala_dataset.py`         | Demonstrates data manipulation and filtering techniques in Scala, providing a foundation for data processing tasks.                                                               | [readme/scala_dataset.md](readme/scala_dataset.md)                      |
| `scala_spark_rdd.py`       | Utilizes PySpark RDDs to perform equivalent data transformations and aggregations as demonstrated in a Scala-based exercise, showcasing basic RDD operations and language translation.                 | [readme/scala_spark_rdd.md](readme/scala_spark_rdd.md)                   |
| `spark_dataframe.py`       | Showcases various data analysis techniques with Spark DataFrames, performing data aggregations and joins on the music dataset.                                                     | [readme/spark_dataframe.md](readme/spark_dataframe.md)                  |
| `spark_graph_dataframe.py` | Builds and analyzes social network graphs using the music dataset. Includes weight calculations, graph construction, and triangle counting with a optimized algorithm and dataframes.                                       | [readme/spark_graph_dataframe.md](readme/spark_graph_dataframe.md)      |
| `spark_graphx.py`          | Implements graph analysis algorithms using Spark GraphX on a Facebook-like social network dataset, emphasizing community detection, neighbor analysis, and distance calculations.                    | [readme/spark_graphx.md](readme/spark_graphx.md)                        |
| `neo4j_asterix.cypher`     | Uses Cypher queries to analyze character relationships, album appearances, and other aspects of the Asterix and Obelix universe, highlighting graph database capabilities.                  | [readme/neo4j_asterix.md](readme/neo4j_asterix.md)                      |

## Getting Started

To run the code in this repository, you will need to:

1.  **Install the Necessary Tools:** Make sure you have Python, Apache Spark, PySpark, Neo4j (if applicable), and any other required libraries installed on your system. Refer to the documentation for each tool for installation instructions.
2.  **Set Up the Environment:** Configure the environment variables and data paths as specified in each project's code.
3.  **Download the Datasets:** Obtain the necessary datasets and place them in the correct locations.
4.  **Run the Scripts:** Execute the Python or Cypher scripts using the appropriate commands.

Please refer to the individual project's documentation for specific instructions on how to set up and run the code.

This repository provides a starting point for learning and experimenting with data analysis and graph database technologies. We encourage you to explore the code, modify it, and adapt it to your own projects and datasets.