DataManagement
==============

The Project is to create two Data Models in MongoDB: Referential and Embedded Model. The dataset to be used is Twitter Dataset from the URL: https://wiki.cites.illinois.edu/wiki/display/forward/Dataset-UDI-TwitterCrawl-Aug2012. The dataset is split into 3: User (containing User Profiles), Network (connections) and Tweets. 

Steps to upload the data into Mongo:

1. Download the data from the link given above.
2. Reformat users.txt to UTF8 format using the script "reformat.sh"
3. Split the "users.txt" file into parts using the python file "ProcessUsersFile.py". This would split the users.txt into 9 files 1.txt, 2.txt, etc into the users folder
4. Run the shell script "UploadUserParallel.sh" which will upload the data into a database named "TwitterDB" and collection "users"
5. Split the "network.txt" file into parts using the python file "ProcessNetworkFile.py". This would split the network.txt into 9 files 1.txt, 2.txt, etc into the network folder.
6. Run the shell script "UploadNetworkParallel.sh" which will upload the data into a database named "TwitterDB" and collection "network"
7. The tweets are put into seperate files named after the user id in the user profile. The tweets need to be converted from their current format into json format. First step is to replace the delimiter "***\n" as the delimiter is present in the Text itself and it caused issues during parsing. Run the script "ParallelReplace.py" which would replace the delimiter with the new delimiter. Run the script "ParallelProcessTweets.py" which would spawn 8 threads. Each thread is then given alloted a subset of files. Thread 1 will processes the files starting with 1, Thread 2 processes the files starting with 2 etc. This would take a few hours to run depending on the cores in the CPU. 
8. To upload the data from referential model to embedded model, run the javascript file, CombineCollections.js. The script would run for a few hours. It took around 15 hours to run on my Mac with 8 GB RAM with a Quad Core Processor.
