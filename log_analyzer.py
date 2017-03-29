from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from operator import add
import argparse




conf = SparkConf().setAppName("Log Analyser")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

textFile = sc.textFile("iliad")

print textFile.count()

def filter_user_achilles(line):
  if "Starting Session" in line and "user achille" in line:
    return line


counts = textFile.flatMap(lambda x:[x]) \
		 .filter(lambda x:filter_user_achilles(x)) \
                 .map(lambda x: (1, 1)) \
                 .reduceByKey(lambda x, y: x + y).collect()

print counts
