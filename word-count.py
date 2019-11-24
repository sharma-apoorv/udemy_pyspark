import findspark
findspark.init()

import re
from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize_words(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())

''' Read in the file and get relevant rdd '''
lines = sc.textFile("Book.txt")
rdd = lines.flatMap(normalize_words)

'''
word_counts = rdd.countByValue()
for w, c in word_counts.items():
    clean_word = w.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, c)
'''

word_counts = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
for w, c in sorted(word_counts, key=lambda x: (x[1], x[0])):
    clean_word = w.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, c)