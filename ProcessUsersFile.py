from joblib import Parallel, delayed  
import multiprocessing
import fnmatch
import os
import glob
import time
import datetime
import re

input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/"
#output_dir="/Users/aravindh/Downloads/tmp/output/"
output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/users/"
#=======================
startTime = time.time()
input_file=input_dir+"UTF8_users.txt"
output_1=output_dir+"1.txt"
output_2=output_dir+"2.txt"
output_3=output_dir+"3.txt"
output_4=output_dir+"4.txt"
output_5=output_dir+"5.txt"
output_6=output_dir+"6.txt"
output_7=output_dir+"7.txt"
output_8=output_dir+"8.txt"
output_9=output_dir+"9.txt"
out_1=open(output_1,"wb")
out_2=open(output_2,"wb")
out_3=open(output_3,"wb")
out_4=open(output_4,"wb")
out_5=open(output_5,"wb")
out_6=open(output_6,"wb")
out_7=open(output_7,"wb")
out_8=open(output_8,"wb")
out_9=open(output_9,"wb")
out_1.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_2.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_3.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_4.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_5.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_6.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_7.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_8.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
out_9.write("user_id\tuser_name\tfriend_count\tfollower_count\tstatus_count\tfavorite_count\taccount_age\tuser_location\n")
with open(input_file) as file:
    for line in file:
        if re.match('^1',line):
            out_1.write(line)
        elif re.match('^2',line):
            out_2.write(line)
        elif re.match('^3',line):
            out_3.write(line)
        elif re.match('^4',line):
            out_4.write(line)
        elif re.match('^5',line):
            out_5.write(line)
        elif re.match('^6',line):
            out_6.write(line)
        elif re.match('^7',line):
            out_7.write(line)
        elif re.match('^8',line):
            out_8.write(line)
        elif re.match('^9',line):
            out_9.write(line)
out_1.close()
out_2.close()
out_3.close()
out_4.close()
out_5.close()
out_6.close()
out_7.close()
out_8.close()
out_9.close()