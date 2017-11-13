-- Q4.PIG: Which are common expressions of online anger?

REGISTER ./tutorial.jar
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
Lowered = FOREACH Tweets GENERATE id, org.apache.pig.tutorial.ToLower(text) as tweet;
Ngram = FOREACH Lowered GENERATE id, flatten(org.apache.pig.tutorial.NGramGenerator(tweet)) as ngram;
Ngram = FILTER Ngram BY SIZE(ngram) > 6;
Ngram = DISTINCT Ngram;
Grouped = GROUP Ngram BY (ngram);
Frequency = FOREACH Grouped GENERATE group, COUNT(Ngram) as count;
MostUsed = FILTER Frequency BY count > 20;
Result = ORDER MostUsed BY count DESC;
-- STORE @ INTO '4_common_expressions';
Top10 = LIMIT Result 10;
DUMP @;