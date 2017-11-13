-- Q2. PIG: Who are those who send abusive tweets?

Tweets = LOAD './woman/tweets.csv' using PigStorage(',') as (
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
UserIds = DISTINCT UserIds;
Users = LOAD './woman/users.csv' using PigStorage(',') as (
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
Result = JOIN Users BY id, UserIds BY user_id;
Result = FOREACH Result GENERATE id, name, screen_name;
Result = DISTINCT Result;
STORE @ INTO '2_Sending_Users';