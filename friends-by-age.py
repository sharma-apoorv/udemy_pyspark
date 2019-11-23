import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("FriendsAge")
sc = SparkContext(conf = conf)

''' Simple function to extract the relevant information that will
 be needed. Returns a tuple of (age, number_of_friends)'''
def parse_fake_friends(line):
    l = line.split(",")
    friend_age = int(l[2])
    friend_num_friends = int(l[3])
    return(friend_age, friend_num_friends)

''' Read in the file and get relevant rdd '''
lines = sc.textFile("fakefriends.csv")
rdd = lines.map(parse_fake_friends)

''' Get the total for each age group and then find the average '''
age_totals = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
friends_avg = age_totals.mapValues(lambda x: int(x[0] / x[1]))
results = friends_avg.collect()

''' Print out simple key->value pair '''
for res in results:
    print(res)