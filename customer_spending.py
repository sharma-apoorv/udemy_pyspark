import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def parse_text(line):
    l = line.split(",")
    customer_id = l[0]
    amount_spent = float(l[2])

    return (customer_id, amount_spent)

''' Read in the file and get relevant rdd '''
lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parse_text)

total_spending = rdd.reduceByKey(lambda x,y: x + y)
results = total_spending.collect()

for res in sorted(results, key=lambda x: (x[1], x[0])):
    print("{cid} spent a total of ${amount:.2f}".format(cid=res[0], amount=res[1]))