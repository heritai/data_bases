# Neo4j Analysis of Asterix and Obelix Graph Dataset
#
# This file contains Cypher queries for analyzing a graph database representing
# characters, albums, and relationships from the Asterix and Obelix comic book series.

# --- Data Loading (Assumed - this code creates the graph) ---
# The following code creates nodes and relationships based on the
# "graphe_asterix.txt" file content
# Each line in the file represents a node or a relationship
# The nodes are: Characters, Character types, Nationalities and Albums
# The relationships are: COMPAGNON_AVENTURE, APPARAIT_DANS, NATIONALITE, PERSONNAGE_TYPE

# Create Nodes:
# (cleopatre {personnageid: 56, name: 'Cleopatre', nationalite: 'Egyptienne', personnagetype: 'Les autres'}),
# ... (rest of the create node statements from graphe_asterix.txt) ...

#Create Relations:
# (cleopatre)-[:COMPAGNON_AVENTURE]->(cesarionPtolemeeXVI),
# ... (rest of the create relationship statements from graphe_asterix.txt) ...

# Important Note: The actual CREATE statements from graphe_asterix.txt would be included here.

# --- Basic Queries ---

# 1. Find the top 3 most related entities by COMPAGNON_AVENTURE/NATIONALITE/PERSONNAGE_TYPE with the number of connections
MATCH (a)-[r]-(b)
WITH a,type(r) as rr,COUNT(*) as deg
WHERE rr="NATIONALITE" or rr="PERSONNAGE_TYPE"
WITH a,sum(deg) as degg
ORDER BY degg DESC LIMIT 3
RETURN a.name,degg

#Explanation:
#This query finds the top 3 characters with the most connections via NATIONALITE or PERSONNAGE_TYPE relationships.
#The queries counts the number of each type of connection then applies a sum aggregation for each of the results, filters only for the specified relations

# 2. Find the shortest path between Jules Cesar and Brutus (up to 10 hops)
MATCH p=(a{name:"Jules Cesar"})-[*..10]-(b{name:"Brutus"})
RETURN p,length(p) as len
ORDER BY len DESC LIMIT 1

#Explanation:
#This query finds any path between Jules Cesar and Brutus with a maximum length of 10 relationships, without restricting the types of relations between them.
#It then orders the results by the length of the path in descending order and returns the longest path.

# 3. Find the shortest path between Jules Cesar and Epidemais
MATCH p=shortestPath((a)-[*1..10]-(b))
WHERE a.name="Jules Cesar" and b.name="Epidemais"
RETURN p,length(p)

#Explanation:
#This query finds the shortest path between Jules Cesar and Epidemais using the shortestPath function.
#It specifies a minimum and maximum path length of 1 to 10 relationships.

# 4. Find Cleopatre's COMPAGNON_AVENTUREs within 3 degrees of separation
MATCH(a{name:"Cleopatre"})-[:COMPAGNON_AVENTURE*3]-(b)
RETURN a,b

#Explanation:
#This query finds all characters who are COMPAGNON_AVENTUREs of Cleopatre within 3 degrees of separation.

# 5. Find COMPAGNON_AVENTUREs of Cleopatre, Jules Cesar, or Caius Obtus, excluding certain characters
MATCH(a:PERSONNAGE)-[:COMPAGNON_AVENTURE]-(b)
WHERE ( a.name="Cleopatre" or a.name="Jules Cesar" or a.name="Caius Obtus") and
(b.name= "Cesarion (Ptolemee XVI)" or b.name= "Caligula Alavacomgetepus")
RETURN a,b

#Explanation:
#This query finds characters who are COMPAGNON_AVENTUREs of Cleopatre, Jules Cesar, or Caius Obtus.

# 6. Find distinct properties of PERSONNAGEs who are NOT COMPAGNON_AVENTUREs of anyone
MATCH(a:PERSONNAGE)
WHERE NOT (a)-[:COMPAGNON_AVENTURE]->()
RETURN DISTINCT properties(a)

#Explanation:
#This query finds all characters that are never the COMPAGNON_AVENTURE

# 7. Find characters and albums where people appears
MATCH(p)-[:APPARAIT_DANS]->(m)<-[:APPARAIT_DANS]-(u)
RETURN p.name,u.name, m.albumid LIMIT 3

#Explanation:
#Finds the relations with the album ids
#Filters all those that the users have those relations, with the limit of 3 entries

# 8. Find triplets of connected characters
MATCH(a)-[]-(b)
MATCH(b)-[]-(c)
WHERE NOT a=b and NOT b=c and NOT a=c
RETURN a.name, b.name, c.name LIMIT 3

#Explanation:
# Finds the conections between the 3 parties
# The function also assures that one node is not the other

# --- Basic Information ---
# Count all nodes
MATCH(n) RETURN COUNT(n)

#Explanation:
#Finds the total number of nodes in the graph

# Count all edges
MATCH ()-[]->() RETURN COUNT(*)

#Explanation:
#Finds the total number of edges in the graph

# --- Node Properties ---
# Find properties of all character and show in the table with a limit
MATCH(n:PERSONNAGE) RETURN properties(n) LIMIT 3

#Explanation:
#Show the atributes on a person, limited to 3 different atributes

# Show label where name = Jules Cesar
MATCH(n) WHERE n.name="Jules Cesar" RETURN labels(n)

#Explanation:
#Show label with the label "Jules Cesar"

# Adding Label
# Adding Label to every PERSONNAGE
MATCH(n) WHERE EXISTS(n.personnageid)
SET n:PERSONNAGE RETURN n.name, labels(n) as labels

#Explanation:
# Adds the "PERSONNAGE" label to all nodes that have a "personnageid" property.

# Display all node atributes

MATCH(n) RETURN n