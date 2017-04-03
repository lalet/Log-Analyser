from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
pyspark.sql.DataFrame 
from operator import add
import argparse




conf = SparkConf().setAppName("Log Analyser")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

textFile = sc.textFile("iliad")

textFileCombined = sc.textFile("iliad,odyssey")

print textFile.count()

def filter_user_achilles(line):
  if "Starting Session" in line and "user achille" in line:
    return line

def filter_find_user(line):
  if  "systemd: Starting Session " in line:
    lines=line.split("user")
    print lines[1]
    return lines[1]

def get_system_name(line):
  words = line.strip().split(" ")
  for word in words:
    if "iliad"==word:
      return 1
    elif "odyssey"==word:
      return 2
  return 0

counts = textFile.flatMap(lambda x:[x]) \
		 .filter(lambda x:filter_user_achilles(x)) \
                 .map(lambda x: (1, 1)) \
                 .reduceByKey(lambda x, y: x + y).collect()

print counts

users = textFile.flatMap(lambda x:[x]) \
                .filter(lambda line:"systemd: Starting Session " in line ) \
                .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
     		.distinct()  \
		.collect()

session = textFile.flatMap(lambda x:[x]) \
                  .filter(lambda line:"systemd: Starting Session " in line ) \
                  .map(lambda line: (str(line.split("user")[-1].strip()[:-1]),1)) \
		  .reduceByKey(lambda x, y: x + y) \
                  .collect()

errors = textFile.flatMap(lambda x:[x]) \
                 .filter(lambda line:"error" in (line.lower()) ) \
                 .map(lambda line: (1,1)) \
                 .reduceByKey(lambda x, y: x + y) \
                 .collect()

error_counts = textFile.flatMap(lambda x:[x]) \
               	       .filter(lambda line:"error" in (line.lower())) \
                       .map(lambda line: (line.split("iliad")[1],1)) \
                       .reduceByKey(lambda x, y: x + y) \
	               .sortBy(lambda x: x[1],ascending=False) \
                       .collect()

#unique_users = textFileCombined.flatMap(lambda x:[x]) \
#               		       .filter(lambda line:"systemd: Starting Session " in line) \
#               		       .map(lambda line:(str(line.split("user")[-1].strip()[:-1]),get_system_name(line))) \
#				       .filter(lambda line:"None" not in line) \
#			       .reduceByKey(lambda a:a+a)  \
#               		       .distinct() \
#			       .collect()

df = textFile.map(lambda r: Row(r)).toDF(["line"])
df_errors = df.filter(col("line").like("%ERROR%"))
df_errors.show()

print users
print session
print errors
print error_counts[0:5]
print unique_users
