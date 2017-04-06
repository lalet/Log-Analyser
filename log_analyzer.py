from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
from operator import add
import argparse




conf = SparkConf().setAppName("Log Analyser")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

textFile = sc.textFile("iliad")

textFileTwo= sc.textFile("odyssey")

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
  print words
  return words[3]

#counts = textFile.flatMap(lambda x:[x]) \
		 #.filter(lambda x:filter_user_achilles(x)) \
                 #.map(lambda x: (1, 1)) \
                 #.reduceByKey(lambda x, y: x + y).collect()

#print counts

#users = textFile.flatMap(lambda x:[x]) \
#                .filter(lambda line:"systemd: Starting Session " in line ) \
#                .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
#     		.distinct()  \
#		.collect()

#session = textFile.flatMap(lambda x:[x]) \
#                  .filter(lambda line:"systemd: Starting Session " in line ) \
#                  .map(lambda line: (str(line.split("user")[-1].strip()[:-1]),1)) \
#		  .reduceByKey(lambda x, y: x + y) \
#                  .collect()

#errors = textFile.flatMap(lambda x:[x]) \
#                 .filter(lambda line:"error" in (line.lower()) ) \
#                 .map(lambda line: (1,1)) \
#                 .reduceByKey(lambda x, y: x + y) \
#                 .collect()

#error_counts = textFile.flatMap(lambda x:[x]) \
#               	       .filter(lambda line:"error" in (line.lower())) \
#                       .map(lambda line: (line.split("iliad")[1],1)) \
#                       .reduceByKey(lambda x, y: x + y) \
#	               .sortBy(lambda x: x[1],ascending=False) \
#                       .collect()

unique_users = textFileTwo.flatMap(lambda x:[x]) \
               		       .filter(lambda line:"systemd: Starting Session " in line) \
               		       .map(lambda line:(str(line.split("user")[-1].strip()[:-1]),"odyssey")) \
			       .filter(lambda line:"None" not in line) \
               		       .distinct() 
			      # .collect()

unique_users_iliad = textFile.flatMap(lambda x:[x]) \
                               .filter(lambda line:"systemd: Starting Session " in line) \
                               .map(lambda line:(str(line.split("user")[-1].strip()[:-1]),"iliad")) \
                               .filter(lambda line:"None" not in line) \
                               .distinct() 
                              # .collect()

combined_rdd = unique_users.union(unique_users_iliad)

multiple_hosts = combined_rdd.reduceByKey(lambda x,y:x+","+y) \
			   .map(lambda x:x[0] if len(x[1].split(",")) > 1 else None)  \
			   .collect()

multiple_hosts=[x for x in multiple_hosts if None!=x]
print multiple_hosts


single_login = combined_rdd.reduceByKey(lambda x,y:x+","+y) \
                           .map(lambda x:x if len(x[1].split(","))==1 else None)  \
                           .collect()

single_login=[x for x in single_login if None!=x]
print single_login

users = textFile.flatMap(lambda x:[x]) \
                .filter(lambda line:"systemd: Starting Session " in line ) \
                .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
                .distinct()  \
		.sortBy(lambda x:x) \
                .collect()


print users

anonymised_file = textFile.flatMap(lambda x:[x]) \
                          .filter(lambda line:"systemd: Starting Session " in line ) \
                          .map(lambda line: line.replace(str(line.split("user")[-1].strip()[:-1]),"user-"+str(users.index(str(line.split("user")[-1].strip()[:-1]))))) \
			  .distinct() \
			  .sortBy(lambda x: x[0]) \
			  .collect()

print anonymised_file
			  

#print session
#print errors
#print error_counts[0:5]
#print unique_users
#print unique_users_iliad
#print unique_users.union(unique_users_iliad)
