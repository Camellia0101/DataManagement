from joblib import Parallel, delayed  
import multiprocessing
import fnmatch
import os
import glob
import time
import datetime
import re

inputs = [1,2,3,4,5,6,7,8,9]
def processInput(i):
    #print "Processing files starting with: ",
    #input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets/"
    #output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets_json/"
    input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets/"
    output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/processed/"
    error_dir="/Users/aravindh/Downloads/tmp/err_output/"
    startsWith=str(i)+"*"
    pattern=re.compile(r"[^a-zA-Z0-9-_@]")
    #=======================
    startTime = time.time()
    file_number=1
    for filename in glob.glob(input_dir+startsWith):
        if not filename.startswith('.'):
            input_file=filename[filename.rfind('/')+1:]
            #print "Input file: ", input_file
            output_file=output_dir+str(input_file)
            out = open(output_file,"wb")
            file = open(filename,"r")
            line = file.readline()
            while line:
                if line == "***\n":
                    out.write("&&&&&&&&&&\n")
                else:
                    out.write(line+"\n")
                line = file.readline()
            out.close()
            file_number=file_number+1
    time2 = time.time()
    print "Number of files processed: ", file_number
    #print 'Conversion took ' % (time.time() - startTime)
    #=======================
    return len(fnmatch.filter(os.listdir(input_dir), startsWith))

num_cores = multiprocessing.cpu_count()

print("numCores = " + str(num_cores))
# input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets/"
# path, dirs, files = os.walk(input_dir).next()
# file_count = len(files)
# print "Number of files to be processed: ", file_count

startTime = time.time()
results = Parallel(n_jobs=num_cores, verbose=5)(delayed(processInput)(i) for i in inputs)
print 'Conversion took ', (time.time() - startTime)

print(results)
