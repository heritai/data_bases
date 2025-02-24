# Analysis of Asterix and Obelix Characters and Albums in Neo4j

This project explores relationships between characters, albums, and other entities in the Asterix and Obelix comic book series using the Neo4j graph database.

## Project Overview

The goal is to create a graph database representing the Asterix universe and then use Cypher queries to extract information and identify relationships. Key tasks include:

1.  **Data Modeling:** Designing a graph schema to represent characters, albums, and their connections.
2.  **Data Loading:** Creating nodes and relationships in Neo4j based on the data.
3.  **Cypher Querying:** Writing Cypher queries to explore the graph and answer specific questions about the Asterix universe.

## Data Sources

The project uses a custom dataset representing characters, albums, and relationships from the Asterix and Obelix comic books. The data is defined in the `graphe_asterix.txt` file.

## Graph Schema

The graph schema includes the following node labels:

*   **PERSONNAGE:** Represents a character in the Asterix universe.
    *   Properties: `personnageid` (integer), `name` (string), `nationalite` (string), `personnagetype` (string)
*   **ALBUM:** Represents an Asterix album.
    *   Properties: `albumid` (integer), `name` (string), `premiereedition` (string)
*   **PERSONNAGE_TYPE:** Represents character types.
    *   Properties: `personnagetypeid` (integer), `name` (string)
*   **NATIONALITE:** Represents nationalities.
    *   Properties: `nationaliteid` (integer), `name` (string)

The graph includes the following relationship types:

*   **COMPAGNON_AVENTURE:** Represents characters who are companions in adventures.
*   **APPARAIT_DANS:** Represents characters who appear in specific albums.
*   **PERSONNAGE_TYPE:** Links a character to a character type.
*   **NATIONALITE:** Links a character to a nationality.

## Example Queries

The `neo4j_asterix.cypher` file contains a series of Cypher queries that demonstrate how to explore the Asterix graph database.

### 1. Find the top 3 most related entities by COMPAGNON_AVENTURE/NATIONALITE/PERSONNAGE_TYPE with the number of connections

```cypher
MATCH (a)-[r]-(b)
WITH a,type(r) as rr,COUNT(*) as deg
WHERE rr="NATIONALITE" or rr="PERSONNAGE_TYPE"
WITH a,sum(deg) as degg
ORDER BY degg DESC LIMIT 3
RETURN a.name,degg
```

This query finds the top 3 characters with the most connections via NATIONALITE or PERSONNAGE_TYPE relationships.

### 2. Find the shortest path between Jules Cesar and Brutus

```cypher
MATCH p=(a{name:"Jules Cesar"})-[*..10]-(b{name:"Brutus"})
RETURN p,length(p) as len
ORDER BY len DESC LIMIT 1
```

This query finds the longest path between Jules Cesar and Brutus with a maximum length of 10 relationships, without restricting the types of relations between them.

### 3. Find the shortest path between Jules Cesar and Epidemais

```cypher
MATCH p=shortestPath((a)-[*1..10]-(b))
WHERE a.name="Jules Cesar" and b.name="Epidemais"
RETURN p,length(p)
```

This query finds the shortest path between Jules Cesar and Epidemais using the `shortestPath` function. It specifies a minimum and maximum path length of 1 to 10 relationships.

### 4. Find Cleopatre's COMPAGNON_AVENTUREs within 3 degrees of separation

```cypher
MATCH(a{name:"Cleopatre"})-[:COMPAGNON_AVENTURE*3]-(b)
RETURN a,b
```

This query finds all characters who are COMPAGNON_AVENTUREs of Cleopatre within 3 degrees of separation.

### 5. Find COMPAGNON_AVENTUREs of Cleopatre, Jules Cesar, or Caius Obtus, excluding certain characters

```cypher
MATCH(a:PERSONNAGE)-[:COMPAGNON_AVENTURE]-(b)
WHERE ( a.name="Cleopatre" or a.name="Jules Cesar" or a.name="Caius Obtus") and
(b.name= "Cesarion (Ptolemee XVI)" or b.name= "Caligula Alavacomgetepus")
RETURN a,b
```

### 6. Find distinct properties of PERSONNAGEs who are NOT COMPAGNON_AVENTUREs of anyone

```cypher
MATCH(a:PERSONNAGE)
WHERE NOT (a)-[:COMPAGNON_AVENTURE]->()
RETURN DISTINCT properties(a)
```

This query finds all characters that are never the COMPAGNON_AVENTURE

### 7. Show album with relations where the people are

```cypher
MATCH(p)-[:APPARAIT_DANS]->(m)<-[:APPARAIT_DANS]-(u)
RETURN p.name,u.name, m.albumid LIMIT 3
```

Finds the relations with the album ids, and filters all those that the users have those relations, with the limit of 3 entries

### 8. Show the relations between all characters, so one does not connect to itself

```cypher
MATCH(a)-[]-(b)
MATCH(b)-[]-(c)
WHERE NOT a=b and NOT b=c and NOT a=c
RETURN a.name, b.name, c.name LIMIT 3
```

### 9. Show properties for Personnage entity

```cypher
MATCH(n:PERSONNAGE) RETURN properties(n) LIMIT 3
```

Show all the atributes to a person, and limits it to the first 3 ones

### 10. Show Label for nodes which name is Jules Cesar

```cypher
MATCH(n) WHERE n.name="Jules Cesar" RETURN labels(n)
```

Returns the attributes with the name `Jules Cesar`

### 11. Show the type of relations from the Domais des Dieux

```cypher
MATCH (n) –[r]->(a{name:"Le Domaine des dieux"}) RETURN type(r)
```

Will bring the labels from every connection to the node where the name if `Le Domaine des dieux`.

### 12. Show number of edges from relation

```cypher
MATCH ()-[]->() RETURN COUNT(*)
```

### 13. Show number of nodes

```cypher
MATCH(n) RETURN COUNT(n)
```

### 14. Update the node to be Personnage

```cypher
MATCH(n) WHERE EXISTS(n.personnageid)
SET n:PERSONNAGE RETURN n.name, labels(n) as labels
```

## Conclusion

This project demonstrates how to create, query, and analyze a graph database representing the Asterix and Obelix universe using Neo4j. The Cypher queries provide valuable insights into the relationships between characters, albums, and other entities, highlighting the power and flexibility of graph databases for knowledge representation and analysis.
