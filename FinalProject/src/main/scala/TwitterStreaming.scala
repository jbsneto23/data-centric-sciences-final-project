package br.ufrn.dimap.forall.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import twitter4j.Status
import twitter4j.User
import twitter4j.Place

object TwitterStreaming {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  val violentWords = Set("maria da penha", "assédio", "feminicídio", "violência doméstica", "violência sexual", "violência familiar", "estupro",
    "assedio", "feminicidio", "violencia domestica", "violencia sexual", "violencia familiar")

  def isViolentTweet(s: Status) = {
    val tweetWords = s.getText.toLowerCase().split(" ")
    val violentTerms = tweetWords.filter(w => violentWords.contains(w))
    !violentTerms.isEmpty
  }

  def tweetsMap(s: Status) = {
    val id = s.getId
    val text = s.getText
    val created_at = s.getCreatedAt
    val in_reply_to_status_id_str = s.getInReplyToStatusId
    val in_reply_to_user_id_str = s.getInReplyToUserId
    val in_reply_to_screen_name = s.getInReplyToScreenName
    val user_id = if (s.getUser != null) s.getUser.getId else null
    val place_id = if (s.getPlace != null) s.getPlace.getId else null
    val quoted_status_id = s.getQuotedStatusId
    val is_quote_status = s.getQuotedStatus != null
    val retweeted_status_id = if (s.getRetweetedStatus != null) s.getRetweetedStatus.getId else null
    val retweet_count = s.getRetweetCount
    val favorite_count = s.getFavoriteCount
    val lang = s.getLang

    (id, text, created_at, in_reply_to_status_id_str, in_reply_to_user_id_str,
      in_reply_to_screen_name, user_id, place_id, quoted_status_id, is_quote_status,
      retweeted_status_id, retweet_count, favorite_count, lang)
  }

  def usersMap(u: User) = {
    val id = u.getId
    val name = u.getName
    val screen_name = u.getScreenName
    val location = u.getLocation
    val url = u.getURL
    val description = u.getDescription
    val verified = u.isVerified
    val followers_count = u.getFollowersCount
    val friends_count = u.getFriendsCount
    val listed_count = u.getListedCount
    val favourites_count = u.getFavouritesCount
    val statuses_count = u.getStatusesCount
    val created_at = u.getCreatedAt
    val utc_offset = u.getUtcOffset
    val time_zone = u.getTimeZone
    val geo_enabled = u.isGeoEnabled
    val lang = u.getLang
    val profile_image_url_https = u.getProfileImageURLHttps

    (id, name, screen_name, location, url, description, verified,
      followers_count, friends_count, listed_count, favourites_count,
      statuses_count, created_at, utc_offset, time_zone, geo_enabled,
      lang, profile_image_url_https)
  }

  def placesMap(p: Place) = {
    val id = p.getId
    val name = p.getName
    val place_type = p.getPlaceType
    val url = p.getURL
    val country = p.getCountry
    val country_code = p.getCountryCode
    val full_name = p.getFullName

    (id, name, place_type, url, country, country_code, full_name)
  }

  def main(args: Array[String]) {

    setupTwitter()

    val ssc = new StreamingContext("local[*]", "TweeterStreaming", Seconds(30))

    setupLogging()

    val streaming = TwitterUtils.createStream(ssc, None)

    val violentTweets = streaming.filter(isViolentTweet).cache()

    val tweets = violentTweets.map(tweetsMap)

    val users = violentTweets.map(s => usersMap(s.getUser))

    val places = violentTweets.filter(s => s.getPlace != null).map(s => placesMap(s.getPlace))

    tweets.map(_.productIterator.mkString(",")).saveAsTextFiles("tweets.csv")
    users.map(_.productIterator.mkString(",")).saveAsTextFiles("users.csv")
    places.map(_.productIterator.mkString(",")).saveAsTextFiles("places.csv")

    //tweets.map(_.productIterator.mkString(",")).print
    //users.map(_.productIterator.mkString(",")).print
    //places.map(_.productIterator.mkString(",")).print

    ssc.checkpoint("../checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

}
