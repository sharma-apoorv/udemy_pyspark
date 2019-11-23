import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import collections

''' Set local context '''
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parse_temp(line):
    l = line.split(",")
    site_id = l[0]
    measurement_type = l[2]
    temp_c = float(int(l[3]) / 10)

    return (site_id, measurement_type, temp_c)

''' Read in the file and get relevant rdd '''
lines = sc.textFile("1800.csv")
rdd = lines.map(parse_temp)

''' Keep only TMIN values ''' 
min_temps = rdd.filter(lambda x: "TMIN" in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_temps = station_temps.reduceByKey(lambda x, y: min(x,y))

''' Keep only TMAX values ''' 
max_temps = rdd.filter(lambda x: "TMAX" in x[1])
station_temps = max_temps.map(lambda x: (x[0], x[2]))
max_temps = station_temps.reduceByKey(lambda x, y: max(x,y))

results_min = min_temps.collect()
results_max = max_temps.collect()

print("Min Temperatures")
for res in results_min:
    print("{site_id}\t{temp:.2f} C".format(site_id=res[0], temp=res[1]))

print("Max Temperatures")
for res in results_max:
    print("{site_id}\t{temp:.2f} C".format(site_id=res[0], temp=res[1]))