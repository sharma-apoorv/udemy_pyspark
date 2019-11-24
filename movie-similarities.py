#Boilerplate stuff:
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from math import sqrt
import sys

conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            l = line.split("|")
            movie_names[int(l[0])] = l[1]
    return movie_names

print("Loading Movie Names")
movie_name_dict = load_movie_names()

''' Read in the file and get relevant rdd '''
lines = sc.textFile("ml-100k/u.data")
movie_ratings = lines.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

def filter_duplicates( t ):
    u_ID, ratings = t[0], t[1]
    
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

''' Join movie data sets and filter out duplicates '''
joined_ratings = movie_ratings.join(movie_ratings)
unique_joined_ratings = joined_ratings.filter(filter_duplicates)

def makePairs(t):
    user, ratings = t[0], t[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

# Now key by (movie1, movie2) pairs.
moviePairs = unique_joined_ratings.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
# Save the results if desired
# moviePairSimilarities.sortByKey()
# moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda x: \
        (x[0][0] == movieID or x[0][1] == movieID) \
        and x[1][0] > scoreThreshold and x[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda x: (x[1], x[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + movie_name_dict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(movie_name_dict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))