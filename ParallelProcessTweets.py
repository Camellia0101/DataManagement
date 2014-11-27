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
    input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/processed/"
    output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets_json/"
    # input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/test_tweets/input/"
    # output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/test_tweets/output/"
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
            err_output_file=error_dir+str(input_file)+".err"
            out = open(output_file,"wb")
            err_out = open(err_output_file,"wb")
            raw = open(filename).read()
            for raw_doc in raw.split("&&&&&&&&&&\n"):
                lines = raw_doc.split("\n")
                lines.pop()
                json_string="{\""
                tweet=' '.join(lines)
                tweet_len=len(tweet)
                if tweet != '':
                    type=tweet[tweet.find("Type:")+5:tweet.find("Origin:")]
                    origin=(tweet[tweet.find("Origin:")+8:tweet.find("Text:")]).lstrip()
                    origin = origin.replace('\"','')
                    origin = re.sub('\s+', ' ', origin)
                    origin=re.sub(r'[^a-zA-Z0-9:\[\]/\._@"]', ' ', origin)
                    tweet=tweet[tweet.find("Text:"):]
                    text=(tweet[tweet.find("Text:")+6:tweet.find("URL: ")]).lstrip()
                    text=text.replace('\"','')
                    text = re.sub('\s+', ' ', text)
                    text=re.sub(r'[^a-zA-Z0-9:\[\]/\._@]', ' ', text)
                    tweet=tweet[tweet.find("URL:"):]
                    url=tweet[tweet.find("URL: ")+5:tweet.find("ID:")]
                    url = url.replace('\"','')
                    id=(tweet[tweet.find("ID:")+4:tweet.find("Time:")]).lstrip()
                    ttime=tweet[tweet.find("Time:")+6:tweet.find("RetCount:")]
                    RetCount=tweet[tweet.find("RetCount:")+10:tweet.find("Favorite:")]
                    favorite=tweet[tweet.find("Favorite:")+10:tweet.find("MentionedEntities:")]
                    mentioned_entities=tweet[tweet.find("MentionedEntities:")+19:tweet.find("Hashtags:")]
                    hashtags=(tweet[tweet.find("Hashtags:")+10:tweet_len]).lstrip()
                    json_string=json_string+"UserID\":"+input_file+",\""
                    json_string=json_string+"Type\":\""+type.rstrip()+"\""+",\"Origin\":\""+origin.rstrip()+"\",\"Text\":\""
                    json_string=json_string+text.rstrip()+"\",\"URL\":\""+url.rstrip()+"\",\"ID\":"+id.rstrip()+",\"Time\":\""
                    json_string=json_string+ttime.rstrip()+"\",\"MentionedEntities\":\""+mentioned_entities.rstrip()+"\",\"RetCount\":"
                    json_string=json_string+RetCount.rstrip()+",\"Favorite\":\""+favorite.rstrip()+"\",\"Hashtags\":\""+hashtags.rstrip()+"\"}"+"\n"
                    out.write(json_string)
            out.close()
            err_out.close()
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
