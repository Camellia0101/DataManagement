__author__ = 'aravindh'

import ujson
import glob
import pymongo
import time
import os
import sys
from joblib import Parallel, delayed
import multiprocessing

# from pymongo import MongoClient

inputs = [1,2,3,4,5,6,7,8,9]

def processInput(i):
    db = pymongo.MongoClient('localhost',29017).TwitterDB
    input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets_json/"
    error_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/bulkinsert_error/"
    # input_dir="/Users/aravindh/Downloads/tmp/output2/"
    # error_dir="/Users/aravindh/Downloads/tmp/output/"
    startsWith=str(i)+"*"
    print "Processing file starting with: ", str(i)+"\n"
    for filename in glob.glob(input_dir+startsWith):
        if not filename.startswith('.'):
            if os.path.getsize(filename) > 0:
                try:
                    file = open(filename,"r")
                    bulk=[]
                    line = file.readline()
                    while line:
                        # print line
                        json_string = ujson.loads(line)
                        bulk.append(json_string)
                        line = file.readline()
                    db.tweets.insert(bulk)
                except Exception, e:
                    err_output_file=error_dir+str(i)+"_errors.log"
                    err_out = open(err_output_file,"wb")
                    err_out.write("------------------------------------\n")
                    err_out.write("FILENAME: "+filename+"\n")
                    err_out.write(repr(e)+"\n")
                    err_out.write("------------------------------------\n")
                    err_out.close()
                    file.close()
                    pass
            file.close()
    print "Finished with the files starting with: ", str(i)+"\n"


num_cores = multiprocessing.cpu_count()

print("numCores = " + str(num_cores))

startTime = time.time()
results = Parallel(n_jobs=num_cores, verbose=5)(delayed(processInput)(i) for i in inputs)
print 'Bulk Insert took ', (time.time() - startTime)


# db = pymongo.MongoClient('localhost',29017).Twitter
# input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets_json/"
# #input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/test_tweets/output2/"
# startsWith="*"
# startTime = time.time()
# for filename in glob.glob(input_dir+startsWith):
#     if not filename.startswith('.'):
#         if os.path.getsize(filename) > 0:
#             print "Processing file: ", filename
#             file = open(filename,"r")
#             bulk=[]
#             line = file.readline()
#             while line:
#                 print line
#                 json_string = ujson.loads(line)
#                 bulk.append(json_string)
#                 line = file.readline()
#             db.tweets.insert(bulk)
#         file.close()
# print 'Bulk Insert took ', (time.time() - startTime)