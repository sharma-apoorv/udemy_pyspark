import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("PopularSuperhero")
sc = SparkContext(conf = conf)

def load_superhero_names():
    superhero_names = {}
    with open("Marvel-Names.txt", encoding="utf8", errors='ignore') as f:
        for line in f:
            l = line.split('"')
            superhero_names[int(l[0])] = l[1]
    return superhero_names

superhero_names_dict = sc.broadcast(load_superhero_names())

''' Read in the file and get relevant rdd '''
lines = sc.textFile("Marvel-Graph.txt")
superheroes = lines.map(lambda x: ( int(x.split()[0]), len(x.split()-1) )
num_occurences = superheroes.reduceByKey(lambda x,y: x+y)
num_occurences_sort = num_occurences.map(lambda x: (x[1],x[0]))
popular_superhero = num_occurences_sort.sortByKey()

popular_superhero_names = popular_superhero.map(lambda x: (superhero_names_dict.value[x[1]], x[0]))

result = popular_superhero_names.collect()
for r in result:
    print(r)