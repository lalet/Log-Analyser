#!/usr/bin/env python

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
from operator import add
import argparse
import sys

def filter_user_achilles(line):
  if "starting Session" in line.lower() and "user achille" in line.lower():
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

def counts(file):
  counts = file.flatMap(lambda x:[x]) \
		 .filter(lambda x:filter_user_achilles(x)) \
                 .map(lambda x: (1, 1)) \
                 .reduceByKey(lambda x, y: x + y).collect()

  return counts[0][1]

def users(file):
  users = file.flatMap(lambda x:[x]) \
                  .filter(lambda line:"systemd: Starting Session " in line ) \
                  .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
     		  .distinct()  \
		  .collect()
  return users

def session(file):
  session = file.flatMap(lambda x:[x]) \
                    .filter(lambda line:"systemd: Starting Session " in line ) \
                    .map(lambda line: (str(line.split("user")[-1].strip()[:-1]),1)) \
		    .reduceByKey(lambda x, y: x + y) \
                    .collect()
  return session

def errors(file):
  error=None
  errors = file.flatMap(lambda x:[x]) \
                   .filter(lambda line:"error" in (line.lower()) ) \
                   .map(lambda line: (1,1)) \
                   .reduceByKey(lambda x, y: x + y) \
                   .collect()
  if errors:
    error=errors[0][1]
  return error

def error_counts(file,file_name):
  error_counts = textFile.flatMap(lambda x:[x]) \
               	         .filter(lambda line:"error" in (line.lower())) \
                         .map(lambda line: (line.split(file_name)[1],1)) \
                         .reduceByKey(lambda x, y: x + y) \
	                 .sortBy(lambda x: x[1],ascending=False) \
                         .collect()
  return error_counts

def unique_users(file1,file2):
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

def single_login(file1,file2):
  single_login = combined_rdd.reduceByKey(lambda x,y:x+","+y) \
                             .map(lambda x:x if len(x[1].split(","))==1 else None)  \
                             .collect()

  single_login=[x for x in single_login if None!=x]
  print single_login

def users_sorted_list(file1,file2):
  users_sorted_list = textFile.flatMap(lambda x:[x]) \
                              .filter(lambda line:"systemd: Starting Session " in line ) \
                              .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
                              .distinct()  \
		              .sortBy(lambda x:x) \
                              .collect()


  print users_sorted_list

def anonymize_user_names(line):
  for user in users_sorted_list:
    if user in line:
      line = line.replace(user,"user-"+str(users_sorted_list.index(user)))
      return line
  return line

  anonymised_file = textFile.flatMap(lambda x:[x]) \
			  .map(lambda line: anonymize_user_names(line)) \
			  .map(lambda line:anonymize_user_names(line) if "su:" in line else line) \
                          .saveAsTextFile("anonymised")

#print "saved anonymised_file"
			  

#print session
#print errors
#print error_counts[0:5]
#print unique_users
#print unique_users_iliad
#print unique_users.union(unique_users_iliad)ii

def get_file_name(path):
  if "/" in path:
    return str(path.split("/")[-1])
  return str(path)

def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error("ERROR: " + message)
    sys.exit(1)

def main():
    conf = SparkConf().setAppName("Log Analyser")
    sc = SparkContext(conf=conf)
    sqlCtx = SQLContext(sc)
    parser=argparse.ArgumentParser(description="log_analyzer.py" ,formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-q",help="Question number")
    parser.add_argument('-f1',"--file1",help='Path to log file')
    parser.add_argument('-f2',"--file2",help='Path to log file')
    args=parser.parse_args()
    if len(sys.argv) < 2:
	log_error("Script should have minimum two parameters. Usage: ./log_analyzer q <question number> -f1 <filepath> -f2<filepath>")
    question_number=args.q
    if args.file1:
	file1=args.file1
	textFile = sc.textFile(file1)
    if args.file2:
	file2=args.file2
        textFileTwo= sc.textFile(file2)
    if question_number == "1":
      file_name=file1
      file2_name=file2
      print "Number of lines:"
      if file1 is not None:
        print get_file_name(file_name)+": ",textFile.count()
      if file2 is not None:
        print get_file_name(file2_name)+": ",textFileTwo.count()

    if question_number == "2":
      print "Number of sessions of user 'achille'"
      if file1 is not None:
	print get_file_name(file1)+": ",counts(textFile)
      if file2 is not None:
	print get_file_name(file2)+": ",counts(textFileTwo)

    if question_number == "3":
      print "Unique user names"
      if file1 is not None:
        print get_file_name(file1)+": ",users(textFile)
      if file2 is not None:
        print get_file_name(file2)+": ",users(textFileTwo)
    
    if question_number == "4":
      print "Sessions per user"
      if file1 is not None:
        print get_file_name(file1)+": ",session(textFile)
      if file2 is not None:
        print get_file_name(file2)+": ",session(textFileTwo)

    if question_number == "5":
      print "Number of errors"
      if file1 is not None:
        print get_file_name(file1)+": ",errors(textFile)
      if file2 is not None:
        print get_file_name(file2)+": ",errors(textFileTwo)


if __name__=='__main__':
	main()



