//
// Query I: Query to find a string in the "Text" field in "tweets" collection
//
var start = new Date().getTime();
db.tweets.find({$text:{$search:"good"}},{ID:1,UserID:1,_id:0})
var end = new Date().getTime();
var time = end - start;
print('Execution time to find an existing text: ' + time+' msec');

var start = new Date().getTime();
db.tweets.find({$text:{$search:"adfafda"}},{ID:1,UserID:1,_id:0})
var end = new Date().getTime();
var time = end - start;
print('Execution time to find a non-existent text: ' + time+' msec');

//
// Query II:
// Return cumulated retweet counts of all tweets, each of which has at 
// least one hashtag.

var start = new Date().getTime();
db.tweets.aggregate([{$match:{Hashtags:{$ne:""}}},{$project:{_id:0,RetCount:1}},{$group:{_id:"",cumulatedRetweetCount:{$sum:"$RetCount"}}},{$project:{cumulatedRetweetCount:1,_id:0}}])
var end = new Date().getTime();
var time = end - start;
if(time>1000){
    print('Execution time to find cumulated retweet count: ' + time/1000 +' secs');
}else{
    print('Execution time to find cumulated retweet count: '+ time +' msec');
}

//
// Query III:
// Query to find user with highest number of followers and list the 
// names of the followers
//
var start = new Date().getTime();
var userID=db.users.find({},{user_id:1,_id:0}).sort({follower_count:-1}).limit(1);
while (userID.hasNext()){
    var follUserID = db.network.find({userid1:userID.next().user_id},{userid2:1,_id:0})
    while (follUserID.hasNext()){
        db.users.find({user_id : follUserID.next().userid2},{user_name :1,_id:0}).forEach(printjson)
    }
}
var end = new Date().getTime();
var time = end - start;
if(time>1000){
    print('Execution time to find follower names: ' + time/1000 +' secs');
}else{
    print('Execution time to find follower names: '+ time +' msec');
}

//
// Query IV:
// Query to insert a follower for a user and increment the follower count in users collection 
// names of the followers
//

var start = new Date().getTime();
db.network.update({userid1:7000002},{$set:{userid2:7000000}},{upsert:true})
db.users.update({user_id:70000002},{$inc:{follower_count:1}})
var time = end - start;
if(time>1000){
    print('Execution time to insert a follower: ' + time/1000 +' secs');
}else{
    print('Execution time to insert a follower: '+ time +' msec');
}