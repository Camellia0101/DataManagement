import sys
import os
import os.path
import time
import glob
import re

input_dir="/Users/aravindh/Downloads/tmp/input/"
#input_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/tweets/"
output_dir="/Users/aravindh/Downloads/tmp/output/"
error_dir="/Users/aravindh/Downloads/tmp/err_output/"
#output_dir="/Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/TwitterDataset/converted_tweets/"

path, dirs, files = os.walk(input_dir).next()
file_count = len(files)

print "Number of files to be processed: ", file_count

startsWith="*"
#pattern=re.compile('[\W_]')
pattern=re.compile(r"[^a-zA-Z0-9_@]")
space_pattern=re.compile(r"^\s+", re.MULTILINE)

time1 = time.time()
file_number=1
for filename in glob.glob(input_dir+startsWith):
    # for filename in os.listdir(input_dir):
    if not filename.startswith('.'):
        input_file=filename[filename.rfind('/')+1:]
        output_file=output_dir+str(input_file)
        err_output_file=error_dir+str(input_file)+".err"
        #print filename,"--->", input_file, "--->", output_file
        out = open(output_file,"wb")
        err_out = open(err_output_file,"wb")
        raw = open(filename).read()
        for raw_doc in raw.split("***\n"):
            lines = raw_doc.split("\n")
            lines.pop()
            lines_size=len(lines)
            if lines_size > 10:
                json_string="{\""
                tweet=' '.join(lines)
                tweet_len=len(tweet)
                if tweet != '':
                    type=tweet[tweet.find("Type:")+5:tweet.find("Origin:")]
                    origin=(tweet[tweet.find("Origin:")+8:tweet.find("Text:")]).lstrip()
                    origin = re.sub('\s+', ' ', origin)
                    origin=re.sub(r'[^a-zA-Z0-9:\[\]/\._@]', ' ', origin)
                    text=(tweet[tweet.find("Text:")+6:tweet.find("URL:")]).lstrip()
                    text = re.sub('\s+', ' ', text)
                    text=re.sub(r'[^a-zA-Z0-9:\[\]/\._@]', ' ', text)
                    tweet=tweet[tweet.find("URL:"):]
                    url=tweet[tweet.find("URL:")+5:tweet.find("ID:")]
                    id=(tweet[tweet.find("ID:")+4:tweet.find("Time:")]).lstrip()
                    ttime=tweet[tweet.find("Time:")+6:tweet.find("RetCount:")]
                    RetCount=tweet[tweet.find("RetCount:")+10:tweet.find("Favorite:")]
                    favorite=tweet[tweet.find("Favorite:")+10:tweet.find("MentionedEntities:")]
                    hashtags=(tweet[tweet.find("Hashtags:")+10:tweet_len]).lstrip()
                    json_string=json_string+"Type\":\""+type.rstrip()+"\""+",\"Origin\":\""+origin.rstrip()+"\",\"Text\":\""
                    json_string=json_string+text.rstrip()+"\",\"URL\":\""+url.rstrip()+"\",\"ID\":"+id.rstrip()+",\"Time\":\""+ttime.rstrip()+"\",\"RetCount\":"
                    json_string=json_string+RetCount.rstrip()+",\"Favorite\":\""+favorite.rstrip()+"\",\"Hashtags\":\""+hashtags.rstrip()+"\"}"+"\n"
                    out.write(json_string)
            else:
                json_string="{"
                for line in lines:
                    if line != "":
                        try:
                            line=line.replace("\"","'")
                            index=line.index(':')
                            key=line[:index]
                            key = key.strip()
                            value=line[index+1:]
                            value=value.strip()
                            value = re.sub('\s+', ' ', value)
                            value=re.sub(r'[^a-zA-Z0-9:\[\]/\._@]', ' ', value)
                            try:
                                if value.isdigit():
                                    json_string=json_string+"\""+key+"\":"+value+","
                                else:
                                    json_string=json_string+"\""+key+"\":\""+value+"\","
                            except ValueError:
                                print "Internal Value Error"
                        except ValueError:
                            print "ValueError"
                            pass
                json_string=json_string.strip(",")+"}\n"
                if json_string.startswith("{\""):
                    out.write(json_string)
        out.close()
        err_out.close()
        file_number=file_number+1
time2 = time.time()
print "Number of files processed: ", file_number
#print 'Conversion took %10.6f ms' % (time2-time1)

