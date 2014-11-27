/**
 * Created by aravindh on 11/24/14.
 */

// db.users.find({user_id:{$in:[40001987,60003258,30000367,20000024,50000303,7000002,8000302]}}).forEach(function(doc){db.users_subset.insert(doc); });

var MapUserProfile = function(){
    var data = {
        UserID:this.user_id,
        UserName:this.user_name,
        FriendCount:this.friend_count,
        FollowerCount:this.follower_count,
        StatusCount:this.status_count,
        FavoriteCount:this.favorite_count,
        AccountAge:this.account_age,
        UserLocation:this.user_location,
        Followers:[],
        Tweets:[]
    };
    emit(this.user_id,data);
};

var MapNetwork = function(){
    var data = {
        UserID: this.userid1,
        UserName: "0",
        FriendCount:0,
        FollowerCount:0,
        StatusCount:0,
        FavoriteCount:0,
        AccountAge:"0",
        UserLocation:"0",
        Followers:[this.userid2],
        Tweets:[]
    };
    emit(this.userid1,data);
};

var MapTweets = function(){
  var data = {
      UserID: this.UserID,
      UserName: "0",
      FriendCount: 0,
      FollowerCount:0,
      StatusCount:0,
      FavoriteCount:0,
      AccountAge:"0",
      UserLocation:"0",
      Followers: [],
      Tweets:[{
          Type:this.Type,
          Origin:this.Origin,
          Text:this.Text,
          URL:this.URL,
          ID:this.ID,
          Time:this.Time,
          RetCount:this.RetCount,
          Favorite:this.Favorite,
          MentionedEntities:this.MentionedEntities,
          Hashtags:this.Hashtags
      }]
  };
    emit(this.UserID,data);

};

var reduce = function(key, values){
    var data = {
        UserID:0,
        UserName:"0",
        FriendCount:0,
        FollowerCount:0,
        StatusCount:0,
        FavoriteCount:0,
        AccountAge:"0",
        UserLocation:"0",
        Followers:[],
        Tweets:[]
    };
    values.forEach(function(doc){
        data.UserID = doc.UserID;
        data.UserName = doc.UserName;
        data.FriendCount = doc.FriendCount;
        data.FollowerCount = doc.FollowerCount;
        data.StatusCount = doc.StatusCount;
        data.FavoriteCount = doc.FavoriteCount;
        data.AccountAge = doc.AccountAge;
        data.UserLocation = doc.UserLocation;
        if(doc.Followers.length>0){
            data.Followers = data.Followers.concat(doc.Followers);
        }
        if(doc.Tweets.length>0){
            data.Tweets = data.Tweets.concat(doc.Tweets);
        }
    });
    return data;
};

//var finalizeFn = function(key,values){
//    return values;
//}
var start = new Date().getTime();
//
print('Starting Users..')
users_result = db.users.mapReduce(MapUserProfile,reduce,{out:{reduce:'Second'}})
var user_end = new Date().getTime();
var user_time = user_end - start;
if(user_time>1000){
    print('Finished with users in : ' + user_time/1000 +' secs');
}else{
    print('Finished with users in : '+ user_time +' msec');
}
print('-------------------------------------------')
//
print('Starting Network..')
var network_start = new Date().getTime();
network_result = db.network.mapReduce(MapNetwork,reduce,{out:{reduce:'Second'}})
var network_end = new Date().getTime();
var network_time = network_end - network_start;
if(network_time>1000){
    print('Finished with network in : ' + network_time/1000 +' secs');
}else{
    print('Finished with nework in : '+ network_time +' msec');
}
print('-------------------------------------------')
//
print('Starting Tweets..')
var tweet_start = new Date().getTime();
tweets_result = db.tweets.mapReduce(MapTweets,reduce,{out:{reduce:'Second'}})
var tweet_end = new Date().getTime();
var tweet_time = tweet_end - tweet_start;
if(tweet_time>1000){
    print('Finished with network in : ' + tweet_time/1000 +' secs');
}else{
    print('Finished with nework in : '+ tweet_time +' msec');
}
print('-------------------------------------------')
//
var end = new Date().getTime();
var time = end - start;
if(time>1000){
    print('Total Time to combine Collections: ' + time/1000 +' secs');
}else{
    print('Total Time to combine Collections: '+ time +' msec');
}