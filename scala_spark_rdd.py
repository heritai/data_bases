from pyspark import SparkContext, SparkConf

# Set up Spark configuration
conf = SparkConf().setAppName("SparkRDDAnalysis")
sc = SparkContext.getOrCreate(conf=conf)

#------------------------------------------------------------------------------
# Helper functions (Scala list exercises translated to Python)
#------------------------------------------------------------------------------

def maxEntiers(liste):
    """Finds the maximum integer in a list."""
    return reduce(lambda a, b: a if a > b else b, liste)

def scEntiers(myList):
    """Calculates the sum of squares of integers in a list."""
    return reduce(lambda a, b: a + b, map(lambda x: x*x, myList))

def moyEntiers(myList):
    """Calculates the average of integers in a list."""
    return float(reduce(lambda a, b: a + b, myList)) / len(myList)

#------------------------------------------------------------------------------
# Define function to validate data before processing
#------------------------------------------------------------------------------

def safe_int(value):
    """Safely converts a string to an integer."""
    try:
        return int(value)
    except ValueError:
        return None

#------------------------------------------------------------------------------
# Data Analysis - Temperature Data
#------------------------------------------------------------------------------
def analyze_temperature_data():
    """Analyzes temperature data from a list of strings."""

    listeTemp = ["7,2010,04,27,75", "12,2009,01,31,78", "41,2009,03,25,95", "2,2008,04,28,76", "7,2010,02,32,91"]
    temp2009 = [safe_int(x.split(",")[3]) for x in listeTemp if x.split(",")[1] == "2009"]
    temp2009 = [temp for temp in temp2009 if temp is not None]

    if temp2009:
        print("Max temperature in 2009: " + str(maxEntiers(temp2009)))
        print("Average temperature in 2009: " + str(moyEntiers(temp2009)))
    else:
        print("No temperature data available for 2009.")

#------------------------------------------------------------------------------
# Data Analysis - Mixed Data
#------------------------------------------------------------------------------
def analyze_mixed_data():
  """Splits and filters mixed data into separate lists."""

  melange = ["1233,100,3,20171010", "1224,22,4,20171009", "100,lala,comedie", "22,loup,documentaire"]

  # Separate lists into notes and films
  notes = [tuple(x.split(",")) for x in melange if len(x.split(",")) == 4 and 1000 <= int(x.split(",")[0]) <= 2000]
  films = [tuple(x.split(",")) for x in melange if len(x.split(",")) == 3 and 0 <= int(x.split(",")[0]) <= 100]

  print('Notes',notes)
  print('Films',films)

#------------------------------------------------------------------------------
# Data transformation with classes -
#------------------------------------------------------------------------------
def transform_person_data():
    """
    This function transforms a list of persons into a list of objects by first defining the classes
    """
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

    print('Valid persons',valid_persons)

#------------------------------------------------------------------------------
# Data Analysis - Books Dataset
#------------------------------------------------------------------------------

class Users:
    """Represents user data."""
    def __init__(self, userid, country, age):
        self.userid = int(userid)
        self.country = country
        self.age = int(age)

    def __repr__(self): #For better printing of the object
        return f"Users(userid={self.userid}, country='{self.country}', age={self.age})"

class Books:
    """Represents book data."""
    def __init__(self, bookid, titlewords, authorwords, year, publisher):
        self.bookid = int(bookid)
        self.titlewords = int(titlewords)
        self.authorwords = int(authorwords)
        self.year = int(year)
        self.publisher = int(publisher)

    def __repr__(self):
        return f"Books(bookid={self.bookid}, titlewords={self.titlewords}, authorwords={self.authorwords}, year={self.year}, publisher={self.publisher})"

class Ratings:
    """Represents rating data."""
    def __init__(self, userid, bookid, rating):
        self.userid = int(userid)
        self.bookid = int(bookid)
        self.rating = int(rating)

    def __repr__(self):
        return f"Ratings(userid={self.userid}, bookid={self.bookid}, rating={self.rating})"


def analyze_books_dataset():
    """Analyzes the books dataset using Spark RDDs."""
    path = "/dataset/Books/" # Correct path to your dataset

    # Load the data
    books_data = sc.textFile(path + "books.csv")
    users_data = sc.textFile(path + "users.csv")
    ratings_data = sc.textFile(path + "ratings.csv")

    # Function to safely remove the spaces from the string values

    # Create RDDs of objects
    Users_header=users_data.first()
    users = users_data.filter(lambda x: x != Users_header) \
                       .map(lambda line: line.split(",")) \
                       .map(lambda parts: Users(userid=parts[0].strip(), country=parts[1].strip(), age=safe_int(parts[2].strip())))

    Books_header=books_data.first()
    books = books_data.filter(lambda x: x != Books_header) \
                       .map(lambda line: line.split(",")) \
                       .map(lambda parts: Books(bookid=parts[0].strip(), titlewords=parts[1].strip(), authorwords=parts[2].strip(), year=parts[3].strip(), publisher=parts[4].strip()))

    Rating_header=ratings_data.first()
    ratings = ratings_data.filter(lambda x: x != Rating_header) \
                         .map(lambda line: line.split(",")) \
                         .map(lambda parts: Ratings(userid=parts[0].strip(), bookid=parts[1].strip(), rating=parts[2].strip()))

    france_users = users.filter(lambda user: user.country == 'france').map(lambda user: user.userid)
    books_2000 = books.filter(lambda book: book.year == 2000).map(lambda book: book.bookid)
    notes = ratings.map(lambda rating: (rating.bookid,safe_int(rating.rating)))
    notes_filtered = notes.filter(lambda rating: rating[1] is not None and rating[1] > 3).map(lambda rating: rating[0])

    # Calculate number of users by country and getting the count from the output
    user_counts_by_country = users.map(lambda user: (user.country, 1)) \
                                  .reduceByKey(lambda a, b: a + b) \
                                  .sortBy(lambda x: x[1], ascending=False)

    top_country = user_counts_by_country.first()

    # Getting the most published year in the dataset
    book_counts_by_year = books.map(lambda book: (book.year, 1)) \
                                 .reduceByKey(lambda a, b: a + b) \
                                 .sortBy(lambda x: x[1], ascending=False)
    top_year = book_counts_by_year.first()

    # ---------------------
    # Queries with Join
    # ---------------------

    # Publishers of books rated by users living in France

    #Map the ratings and users rdds to key value pairs for the join, using the userId as the key
    ratings_kv = ratings.map(lambda rating : (rating.userid, rating.bookid))
    users_kv = users.map(lambda user : (user.userid, user.country))

    #Perform the Join
    joined_rdd = ratings_kv.join(users_kv)

    #Filter for the users with country France
    france_ratings = joined_rdd.filter(lambda record : record[1][1] == 'france')

    #Transform with map to extract the book ids
    france_bookids = france_ratings.map(lambda record : record[1][0])

    #Map the book ids with the books, then do the join, and extract the publisher
    book_kv = books.map(lambda book : (book.bookid, book.publisher))

    #Make another join to extract the books with ratings from french users
    france_publishers_rdd = france_bookids.map(lambda id : (id,1)).join(book_kv)

    #Extract only the publishers and apply the distinct
    france_publishers = france_publishers_rdd.map(lambda record : record[1][1]).distinct()

    # Publishers of books not rated by users living in France
    non_france_ratings = joined_rdd.filter(lambda record : record[1][1] != 'france')

    #Transform with map to extract the book ids
    non_france_bookids = non_france_ratings.map(lambda record : record[1][0]).distinct()

    #Extract only the publishers and apply the distinct
    non_france_publishers_rdd = non_france_bookids.map(lambda id : (id,1)).join(book_kv)

    non_france_publishers = non_france_publishers_rdd.map(lambda record : record[1][1]).distinct()

    # For each book, the average age of users who rated it
    book_user = ratings.map(lambda rating : (rating.userid, rating.bookid))
    age_book = users.map(lambda user : (user.userid, user.age))

    book_users_join = book_user.join(age_book)

    book_users_ages = book_users_join.map(lambda record : (record[1][0], record[1][1]))

    #Now we need to group by the book, then compute the average age of the users, then map each to the bookid
    #Map all the data into key values with bookId the key
    book_average_rdd = book_users_ages.map(lambda pair: (pair[0], (pair[1], 1)))

    #Then we need to reduce by key
    book_average_rdd = book_average_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    #Now we can compute the averages
    book_average = book_average_rdd.map(lambda record : (record[0], record[1][0]/record[1][1]))

    print("\nPublishers of books rated by users living in France:")
    for publisher in france_publishers.take(10):
        print(publisher)

    print("\nPublishers of books NOT rated by users living in France:")
    for publisher in non_france_publishers.take(10):
        print(publisher)

    print("\nFor each book, the average age of users who rated it:")
    for result in book_average.take(10):
        print(result)

#------------------------------------------------------------------------------
# Main
#------------------------------------------------------------------------------

# Perform the analyses
print("\n--- Scala List Exercises (Translated) ---")
listeEntiers = list(range(1, 11))
print("List of integers:", listeEntiers)
print("Max integer:", maxEntiers(listeEntiers))
print("Sum of squares:", scEntiers(listeEntiers))
print("Average:", moyEntiers(listeEntiers))

print("\n--- Temperature Data Analysis ---")
analyze_temperature_data()

print("\n--- Mixed Data Analysis ---")
analyze_mixed_data()

print("\n--- People data ---")
transform_person_data()

print("\n--- Books Dataset Analysis ---")
analyze_books_dataset()

# Stop SparkContext (Good practice to release resources)
sc.stop()