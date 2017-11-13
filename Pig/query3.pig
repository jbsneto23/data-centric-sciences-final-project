-- Q3. PIG: What is the volume of abusive tweets per perpetrator?

Users = LOAD 'hdfs://master:54310/user/hduser/twitter/womensday/users' using PigStorage(',') as (
      id: chararray, 
      name: chararray, 
      screen_name: chararray, 
      location: chararray, 
      url: chararray, 
      description: chararray, 
      verified: boolean,
      followers_count: int,
      friends_count: int,
      listed_count: int, 
      favourites_count: int, 
      statuses_count: int, 
      created_at: chararray, 
      utc_offset: int,
      time_zone: chararray, 
      geo_enabled: boolean,
      lang: chararray, 
      profile_image_url_https: chararray
);
UserInfo = FOREACH Users GENERATE id, screen_name; 
UniqUsers = DISTINCT UserInfo;
Tweets = LOAD 'hdfs://master:54310/user/hduser/twitter/womensday/tweets' using PigStorage(',') as (
      id: chararray, 
      text: chararray, 
      created_at: chararray, 
      in_reply_to_status_id_str: chararray, 
      in_reply_to_user_id_str: chararray, 
      in_reply_to_screen_name: chararray, 
      user_id: chararray, 
      place_id: chararray, 
      quoted_status_id: chararray, 
      is_quote_status: boolean,
      retweeted_status_id: chararray, 
      retweet_count: int,
      favorite_count: int,
      lang: chararray
);
UserIds = FOREACH Tweets GENERATE user_id;
Joined = JOIN UniqUsers BY id, UserIds BY user_id;
Grouped = GROUP Joined By screen_name;
Result = FOREACH Grouped GENERATE group as screen_name, COUNT(Joined) as qty;
ResultSorted = ORDER Result BY qty DESC;
-- STORE @ INTO '3_Abusive_tweets_volume';
Top10 = LIMIT ResultSorted 10;
DUMP @;
