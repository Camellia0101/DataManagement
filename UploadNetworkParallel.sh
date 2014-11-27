#!/bin/sh

now="$(date)"
printf "Current date and time %s\n" "$now"
#
start=`date +%s`
for File in network/*
do
echo $File
sh /Users/aravindh/Documents/Aravindh/Masters/ThirdSemester/B669-SDM/Project/I590-TwitterDataSet/bin/import_large.sh TwitterDB network tsv $File 29017 >$File.out & 
done
end=`date +%s`
runtime=$((end-start))
#
now="$(date)"
printf "Current date and time %s\n" "$now"
#
echo $runtime
