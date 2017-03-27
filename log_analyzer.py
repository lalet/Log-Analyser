from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse

conf = SparkConf().setAppName("Log Analyser")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

textFile = sc.textFile("iliad")

print textFile.count()
