from joblib import Parallel, delayed  
import multiprocessing
import fnmatch
import os
import glob
import time
import datetime
import re

inputs = [1,2,3,4,5,6,7,8,9]
#inputs=[9]
def processInput(i):
    input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets_json/"
    #output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets_json/"
    maxUserID = 0
    startsWith=str(i)+"*"
    pattern=re.compile(r"[^a-zA-Z0-9-_@]")
    #=======================
    file_number=1
    for filename in glob.glob(input_dir+startsWith):
        if not filename.startswith('.'):
            input_file=filename[filename.rfind('/')+1:]
            if long(input_file) > maxUserID:
                maxUserID = long(input_file)
            #print "Input file: ", input_file
            file_number=file_number+1
    #print "Number of files processed: ", file_number
    print "Largest UserID for "+str(i)+" :", maxUserID
    minUserID = maxUserID
    for filename in glob.glob(input_dir+startsWith):
        if not filename.startswith('.'):
            input_file=filename[filename.rfind('/')+1:]

            if long(input_file) < minUserID:
                minUserID = long(input_file)
            #print "Input file: ", input_file
            file_number=file_number+1
    print "Least UserID for "+str(i)+" :", minUserID
    #=======================
    return len(fnmatch.filter(os.listdir(input_dir), startsWith))

num_cores = multiprocessing.cpu_count()

print("numCores = " + str(num_cores))


startTime = time.time()
results = Parallel(n_jobs=num_cores, verbose=5)(delayed(processInput)(i) for i in inputs)
print 'Conversion took ', (time.time() - startTime)

print(results)
