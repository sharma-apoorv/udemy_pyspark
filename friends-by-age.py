import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("FriendsAge")
sc = SparkContext(conf = conf)

def parse_fake_friends(line):
    l = line.split(",")
    friend_age = int(l[2])
    friend_num_friends = int(l[3])
    return(friend_age, friend_num_friends)

lines = sc.textFile("fakefriends.csv")
rdd = lines.map(parse_fake_friends)
age_totals = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
friends_avg = age_totals.mapValues(lambda x: int(x[0] / x[1]))
results = friends_avg.collect()

for res in results:
    print(res)