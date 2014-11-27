#!/bin/sh

now="$(date)"
printf "Current date and time %s\n" "$now"
#
start=`date +%s`
for File in tweets_json/*
do
echo $File
sh /Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/I590-TwitterDataSet/bin/import_large.sh Twitter tweets json $File 29017 
done
end=`date +%s`
runtime=$((end-start))
#
now="$(date)"
printf "Current date and time %s\n" "$now"
#
echo $runtime
