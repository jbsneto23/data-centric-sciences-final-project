-- Q1. PIG: How much online abuse do women receive online?

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
Hour = FOREACH Tweets GENERATE REGEX_EXTRACT(created_at,'(\\S{3}) (\\S{3}) (\\S{2}) (\\S{2}):(\\S{2}):(\\S{2}) (\\S{5})',4) as (hour:int), '1' as (count:int);
Grouped = GROUP Hour BY hour;
Counting = FOREACH Grouped GENERATE group as hour, COUNT(Hour) as cnt;
CountingSorted = ORDER Counting BY cnt DESC;
-- STORE @ INTO '1_Counting_Abuse';
Top10 = LIMIT CountingSorted 10;
DUMP Top10;


