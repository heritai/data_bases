# PySpark RDD Analysis Project

This project demonstrates data analysis using PySpark RDDs. It covers a range of tasks from basic list operations to more complex data transformations and joins using different datasets.

## Project Overview

The project consists of several independent parts, each designed to illustrate specific concepts and techniques in PySpark. These parts include:

*   **Scala Exercises :** Demonstrates translations of simple Scala code into Python.
*   **Temperature Data Analysis:** This shows data spliting, cleaning, and reduction techniques.
*   **Mixed Data Analysis:** This showcases how to filter data based on specific types.
*   **Data transformation to objects:** This will explain how to represent the data in OOP form.
*   **Books Dataset Analysis:** Illustrates loading, transforming, and joining real-world datasets using RDDs.

## Usage

1.  **Environment Setup:** Ensure you have Spark and PySpark installed and configured correctly.
2.  **Data Location:** Adjust the `path` variables within the script to point to the directories containing the required data files ("wordcount.txt," "books.csv," "users.csv," "ratings.csv"). 
3.  **Execution:** Run the `scala_spark_rdd.py` script using `spark-submit`.

## Data Sources

The script utilizes the following datasets:

*   **wordcount.txt:** A simple text file used for word counting.
*   **books.csv, users.csv, ratings.csv:** Datasets providing information about books, users, and their ratings.

## Code Structure

The script is organized into logical sections, each addressing a specific analysis task.

### Helper Functions (Scala List Exercises Translated)

```python
def maxEntiers(liste):
    """Finds the maximum integer in a list."""
    return reduce(lambda a, b: a if a > b else b, liste)

def scEntiers(myList):
    """Calculates the sum of squares of integers in a list."""
    return reduce(lambda a, b: a + b, map(lambda x: x*x, myList))

def moyEntiers(myList):
    """Calculates the average of integers in a list."""
    return float(reduce(lambda a, b: a + b, myList)) / len(myList)
```

These functions demonstrate basic list operations equivalent to the original Scala code.

### Analyzing temperature data

```python
    #Calculate max and average temperature for the year 2009
    listeTemp = ["7,2010,04,27,75", "12,2009,01,31,78", "41,2009,03,25,95", "2,2008,04,28,76", "7,2010,02,32,91"]
    temp2009 = [safe_int(x.split(",")[3]) for x in listeTemp if x.split(",")[1] == "2009"]
    temp2009 = [temp for temp in temp2009 if temp is not None]

    if temp2009:
        print("max des temps 2009 " + str(maxEntiers(temp2009)))
        print("moy des temps 2009 " + str(moyEntiers(temp2009)))
    else:
        print('No temparatures available')
```

This function splits the data then performs the calculations for the 2009 data only.

### Mixed Data - Notes and Films
```python
    #Separate lists into notes and films
    melange = ["1233,100,3,20171010", "1224,22,4,20171009", "100,lala,comedie", "22,loup,documentaire"]

    notes = [tuple(x.split(",")) for x in melange if len(x.split(",")) == 4 and 1000 <= int(x.split(",")[0]) <= 2000]
    films = [tuple(x.split(",")) for x in melange if len(x.split(",")) == 3 and 0 <= int(x.split(",")[0]) <= 100]

    print('Notes',notes)
    print('Films',films)
```

This section demonstrates the separation of the notes and films using the properties shown.

### Transform into objects
```python
    #Transform the Data to Objects
    personnes = [("Joe", "etu", 3), ("Lee", "etu", 4), ("Sara", "ens", 10), ("John", "ens", 5), ("Bill", "nan",20)]

    #Define the class with string fields
    class Etu:
        def __init__(self, nom: str, annee: int):
            self.nom = nom
            self.annee = annee

        def __repr__(self): #For better printing of the object
            return f"Etu(nom={self.nom}, annee={self.annee})"

    class Ens:
        def __init__(self, nom: str, annee: int):
            self.nom = nom
            self.annee = annee

        def __repr__(self): #For better printing of the object
            return f"Ens(nom={self.nom}, annee={self.annee})"

    #Filter out the data we don't need
    valid_persons = [
        Etu(nom, annee) if type == "etu" else Ens(nom, annee)
        for (nom, type, annee) in personnes
        if type in ("etu", "ens")
    ]
```
It presents the transformations to the objects.

### Analyzing the Books Dataset

The `analyze_books_dataset` function demonstrates loading, cleaning, transformation, and analysis of the books dataset using RDDs:

*   Loads the data from `books.csv`, `users.csv`, and `ratings.csv`.
*   Creates RDDs of `Users`, `Books`, and `Ratings` objects.
*   Calculates statistics and performs joins to answer several queries.
*   It now removes the headers using filter.
*   The strings are processed before with the strip function to eliminate blank spaces and to cast the values into objects

### Key RDD Operations

The script makes extensive use of RDD transformations and actions:

*   **`map()`:** Used to apply functions to each element of the RDDs for data extraction and conversion.

    ```python
    users = users_data.map(lambda line: line.split(",")) \
                       .map(lambda parts: Users(userid=parts[0], country=parts[1], age=safe_int(parts[2])))
    ```

*   **`filter()`:** Used to select data based on specific criteria (e.g., users from France).

    ```python
    france_users = users.filter(lambda user: user.country == 'france').map(lambda user: user.userid)
    ```

*   **`reduceByKey()`:** Used to aggregate data based on a key (e.g., counting users by country).

    ```python
    user_counts_by_country = users.map(lambda user: (user.country, 1)) \
                                  .reduceByKey(lambda a, b: a + b)
    ```

*   **`join()`:** Used to combine data from multiple RDDs based on a common key (e.g., joining user data with rating data).
      ```python
      # Perform the Join
      joined_rdd = ratings_kv.join(users_kv)
      ```

## Conclusion

This project provides a practical demonstration of PySpark RDDs for data analysis, offering a foundation for further exploration and more complex projects. The script shows list manipulation, cleaning operations, filtering functions, creation of objects and also transformations of data in order to extract insights about it. By combining all the steps together we have a data processing pipeline which will extract some insights about the original data.
