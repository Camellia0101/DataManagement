//
// Query I: Query to find a string in the "Text" field in combined collection
//
var start = new Date().getTime();
db.Second.find({$text:{$search:"good"}},{UserID:1})
var end = new Date().getTime();
var time = end - start;
print('Execution time to find an existing text: ' + time+' msec');

var start = new Date().getTime();
db.Second.find({$text:{$search:"adfafda"}},{UserID:1})
var end = new Date().getTime();
var time = end - start;
print('Execution time to find a non-existent text: ' + time+' msec');

//
// Query II:
// Return cumulated retweet counts of all tweets, each of which has at 
// least one hashtag.

var start = new Date().getTime();
db.Second.aggregate([
    {$project:{_id:1,'value.UserID':1,'value.Tweets.Hashtags':1,'value.Tweets.RetCount':1}},
    {$unwind:'$value.Tweets'},
    {$match:{'value.Tweets.Hashtags':{$ne:""}}},
    {$group:{_id:"",CumRetCount:{$sum:'$value.Tweets.RetCount'}}},
    {$project:{CumRetCount:1,_id:0}}
    ])
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
var userID=db.Second.find({},{_id:1}).sort({"value.FollowerCount":-1}).limit(1)
while (userID.hasNext()){
    var follUserID=db.Second.find({_id:userID.next()._id},{"value.Followers":1,_id:0})
    while(follUserID.hasNext()){
        var followers=follUserID.next().value.Followers;
        db.Second.find({_id : { $in: followers}},{"value.UserName":1, _id:0}).forEach(printjson)
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

db.Second.update(
    { _id: 17289517 },
    {
        $push : { "value.Friends" : 70000000 },
        $inc : {"value.FriendCount" : 1}
    }
)

var time = end - start;
if(time>1000){
    print('Execution time to insert a follower: ' + time/1000 +' secs');
}else{
    print('Execution time to insert a follower: '+ time +' msec');
}