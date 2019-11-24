import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("MovieRatings")
sc = SparkContext(conf = conf)

def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            l = line.split("|")
            movie_names[int(l[0])] = l[1]
    return movie_names

movie_name_dict = sc.broadcast(load_movie_names())

''' Read in the file and get relevant rdd '''
lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movie_count = movies.reduceByKey(lambda x, y: x + y)
rating_sort = movie_count.map(lambda x: (x[1],x[0]))
sorted_movies = rating_sort.sortByKey()

sorted_movies_names = sorted_movies.map(lambda x: (movie_name_dict.value[x[1]], x[0]))

result = sorted_movies_names.collect()
for r in result:
    print(r)

