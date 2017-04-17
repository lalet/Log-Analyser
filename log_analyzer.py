#!/usr/bin/env python

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
from operator import add
import argparse
import sys
import os
import shutil

#Filter function for fomd the number of times achilles loggedin
def filter_user_achilles(line):
  if "starting session" in line.lower() and "user achille" in line.lower():
    return line

#Function to filter the user
def filter_find_user(line):
  if  "systemd: Starting Session " in line:
    lines=line.split("user")
    return lines[1]

#Function to find the number of logins of user achilles
def counts(file_text):
  counts = file_text.flatMap(lambda x:[x]) \
		 .filter(lambda x:filter_user_achilles(x)) \
                 .map(lambda x: (1, 1)) \
                 .reduceByKey(lambda x, y: x + y).collect()
  if counts:
    return counts[0][1]

def users(file_content):
  users = file_content.flatMap(lambda x:[x]) \
                  .filter(lambda line:"systemd: starting session " in line.lower() ) \
                  .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
     		  .distinct()  \
		  .collect()
  return users

#Funciton to find the number of session by each user
def session(file_content):
  session = file_content.flatMap(lambda x:[x]) \
                        .filter(lambda line:"systemd: starting session " in line.lower() ) \
                        .map(lambda line: (str(line.split("user")[-1].strip()[:-1]),1)) \
		        .reduceByKey(lambda x, y: x + y) \
                        .collect()
  return session

#Function to find the total number of errors
def errors(file_content):
  error=None
  errors = file_content.flatMap(lambda x:[x]) \
                       .filter(lambda line:"error" in (line.lower()) ) \
                       .map(lambda line: (1,1)) \
                       .reduceByKey(lambda x, y: x + y) \
                       .collect()
  if errors:
    error=errors[0][1]
  return error

#Function to find the top 5 errors which occurs frequently
def error_counts(file,file_name):
  error_counts = file.flatMap(lambda x:[x]) \
               	     .filter(lambda line:"error" in (line.lower())) \
                     .map(lambda line: (line.split(file_name)[1] if len(line.split(file_name))>=2 else None,1)) \
		     .filter(lambda line:line is not None) \
                     .reduceByKey(lambda x, y: x + y) \
	             .sortBy(lambda x: x[1],ascending=False) \
                     .collect()
  return error_counts


#function which returns the combined files from two different log files
def get_combined_rdd(file1,file_name,file2,file2_name):
  unique_users = file2.flatMap(lambda x:[x]) \
               		    .filter(lambda line:"systemd: starting session " in line.lower()) \
               		    .map(lambda line:(str(line.split("user")[-1].strip()[:-1]),file2_name)) \
			    .filter(lambda line:line is not None) \
               		    .distinct() 
			      # .collect()

  unique_users_iliad = file1.flatMap(lambda x:[x]) \
                               .filter(lambda line:"systemd: Starting Session " in line) \
                               .map(lambda line:(str(line.split("user")[-1].strip()[:-1]),file_name)) \
                               .filter(lambda line:line is not None) \
                               .distinct() 
                              # .collect()

  return unique_users.union(unique_users_iliad)

#Function to fin users who logged in from multiple systems
def get_multiple_host_users(combined_rdd):  
  multiple_hosts = combined_rdd.reduceByKey(lambda x,y:x+","+y) \
			   .map(lambda x:x[0] if len(x[1].split(",")) > 1 else None)  \
			   .filter(lambda line:line is not None) \
			   .collect()

  return multiple_hosts

#Function to find the users which logged in only from one system
def get_single_host_users(combined_rdd):
  single_login = combined_rdd.reduceByKey(lambda x,y:x+","+y) \
                             .map(lambda x:x if len(x[1].split(","))==1 else None)  \
			     .filter(lambda line:line is not None) \
                             .collect()

  return single_login

#Function which finds all the users and sorts the users 
def get_users_sorted_list(file_content):
  users_sorted_list = file_content.flatMap(lambda x:[x]) \
                          .filter(lambda line:"systemd: Starting Session " in line ) \
                          .map(lambda line: str(line.split("user")[-1].strip()[:-1])) \
                          .distinct()  \
		          .sortBy(lambda x:x) \
                          .collect()

  return users_sorted_list


#Function to anonymize the user names in log files and write as a separate file
def anonymize_user_names(line,users_sorted_list):
  for user in users_sorted_list:
    if user in line:
      line = line.replace(user,"user-"+str(users_sorted_list.index(user)))
      return line
  return line

def anonymize_file(file_content,file_name,users_sorted_list):
  anonymised_file = file_content.flatMap(lambda x:[x]) \
		                .map(lambda line: anonymize_user_names(line,users_sorted_list)) \
			        .map(lambda line:anonymize_user_names(line,users_sorted_list) if "su:" in line else line) \
  				.saveAsTextFile("anonymised-files-"+file_name) 
  print "saved anonymised_file","anonymised-files-"+file_name
			  

#print session
#print errors
#print error_counts[0:5]
#print unique_users
#print unique_users_iliad
#print unique_users.union(unique_users_iliad)ii

#Function to get the file name from the path
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
    file1=None
    file2=None
    if args.file1:
	file1=args.file1
	textFile = sc.textFile(file1)
    if args.file2:
	file2=args.file2
        textFileTwo= sc.textFile(file2)
    if question_number == "1":
      print "Number of lines:"
      if file1 is not None:
        print get_file_name(file1)+": ",textFile.count()
      if file2 is not None:
        print get_file_name(file2)+": ",textFileTwo.count()

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

    if question_number == "6":
      print "5 most frequent errors"
      if file1 is not None:
	error_list=error_counts(textFile,get_file_name(file1))
        print get_file_name(file1)+": \n"
	for error in error_list[0:5]:
	  print "- ",error[1],error[0]
      if file2 is not None:
	#error_counts(textFileTwo,get_file_name(file2))
	error_list=error_counts(textFileTwo,get_file_name(file2))
        print get_file_name(file2)+": \n"
	for error in error_list[0:5]:
          print "- ",error[1],error[0]
    
    if question_number == "7":
      print "Users who started a session on exactly two hosts:"
      if file1 is not None and file2 is not None:
	combined_rdd=get_combined_rdd(textFile,get_file_name(file1),textFileTwo,get_file_name(file2))
        print get_multiple_host_users(combined_rdd)
      else:
	log_error("This needs exactly two files for finding the users who logged in from multiple systems")  
 
    if question_number == "8":
      print "Users who started a session on exactly one host:"
      if file1 is not None and file2 is not None:
        combined_rdd=get_combined_rdd(textFile,get_file_name(file1),textFileTwo,get_file_name(file2))
        print get_single_host_users(combined_rdd)
      else:
        log_error("This needs exactly two files for finding the users who logged in from a single system")

    if question_number == "9":
      print "Anonymise files"
      if file1 is not None:
	users_sorted_list=get_users_sorted_list(textFile)
	for user in users_sorted_list:
          print user,"-"," user-",users_sorted_list.index(user)
	if os.path.exists("anonymised-files-"+get_file_name(file1)):
	  shutil.rmtree("anonymised-files-"+get_file_name(file1))
        anonymize_file(textFile,get_file_name(file1),users_sorted_list)
      if file2 is not None:
        users_sorted_list=get_users_sorted_list(textFileTwo)
	print "User-Mapping:"
	for user in users_sorted_list:
	  print user,"-"," user-",users_sorted_list.index(user)
	if os.path.exists("anonymised-files-"+get_file_name(file2)):
          shutil.rmtree("anonymised-files-"+get_file_name(file2))
        anonymize_file(textFileTwo,get_file_name(file2),users_sorted_list)

if __name__=='__main__':
	main()



