package br.ufrn.dimap.forall.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object TwitterInfluenceGraph {

  case class User(id: Long,
                  name: String,
                  screen_name: String,
                  location: String,
                  url: String,
                  description: String,
                  verified: Boolean,
                  followers_count: Int,
                  friends_count: Int,
                  listed_count: Int,
                  favourites_count: Int,
                  statuses_count: Int,
                  created_at: String,
                  utc_offset: Int,
                  time_zone: String,
                  geo_enabled: Boolean,
                  lang: String,
                  profile_image_url_https: String) extends Serializable

  case class Tweet(id: Long,
                   text: String,
                   created_at: String,
                   in_reply_to_status_id_str: String,
                   in_reply_to_user_id_str: String,
                   in_reply_to_screen_name: String,
                   user_id: Long,
                   place_id: String,
                   quoted_status_id: Long,
                   is_quote_status: Boolean,
                   retweeted_status_id: Long,
                   retweet_count: Int,
                   favorite_count: Int,
                   lang: String) extends Serializable

  def parseUsers(line: String): Option[User] = {
    var fields = line.split(",")
    if (fields.length >= 18) {
      try {
        val id: Long = if (!fields(0).trim().isEmpty()) fields(0).trim().toLong else 0L
        val name: String = fields(1)
        val screen_name: String = fields(2)
        val location: String = fields(3)
        val url: String = fields(4)
        val description: String = fields(5)
        val verified: Boolean = if (!fields(6).trim().isEmpty()) fields(6).trim().toBoolean else false
        val followers_count: Int = if (!fields(7).trim().isEmpty()) fields(7).trim().toInt else 0
        val friends_count: Int = if (!fields(8).trim().isEmpty()) fields(8).trim().toInt else 0
        val listed_count: Int = if (!fields(9).trim().isEmpty()) fields(9).trim().toInt else 0
        val favourites_count: Int = if (!fields(10).trim().isEmpty()) fields(10).trim().toInt else 0
        val statuses_count: Int = if (!fields(11).trim().isEmpty()) fields(11).trim().toInt else 0
        val created_at: String = fields(12)
        val utc_offset: Int = if (!fields(13).trim().isEmpty()) fields(13).trim().toInt else 0
        val time_zone: String = fields(14)
        val geo_enabled: Boolean = if (!fields(15).trim().isEmpty()) fields(15).trim().toBoolean else false
        val lang: String = fields(16)
        val profile_image_url_https: String = fields(17)

        return Some(User(id, name, screen_name, location, url, description,
          verified, followers_count, friends_count, listed_count, favourites_count,
          statuses_count, created_at, utc_offset, time_zone, geo_enabled,
          lang, profile_image_url_https))
      } catch {
        case e: Exception => return None
      }
    }
    return None
  }

  def parseTweet(line: String): Option[Tweet] = {
    var fields = line.split(",")
    if (fields.length > 13) {
      try {
        val id: Long = if (!fields(0).trim().isEmpty()) fields(0).trim().toLong else 0L
        val text: String = fields(1)
        val created_at: String = fields(2)
        val in_reply_to_status_id_str: String = fields(3)
        val in_reply_to_user_id_str: String = fields(4)
        val in_reply_to_screen_name: String = fields(5)
        val user_id: Long = if (!fields(6).trim().isEmpty()) fields(6).trim().toLong else 0L
        val place_id: String = fields(7)
        val quoted_status_id: Long = if (!fields(8).trim().isEmpty()) fields(8).trim().toLong else 0L
        val is_quote_status: Boolean = if (!fields(9).trim().isEmpty()) fields(9).trim().toBoolean else false
        val retweeted_status_id: Long = if (!fields(10).trim().isEmpty()) fields(10).trim().toLong else 0L
        val retweet_count: Int = if (!fields(11).trim().isEmpty()) fields(11).trim().toInt else 0
        val favorite_count: Int = if (!fields(12).trim().isEmpty()) fields(12).trim().toInt else 0
        val lang: String = fields(13)
        return Some(Tweet(id, text, created_at, in_reply_to_status_id_str, in_reply_to_user_id_str,
          in_reply_to_screen_name, user_id, place_id, quoted_status_id, is_quote_status,
          retweeted_status_id, retweet_count, favorite_count, lang))
      } catch {
        case e: Exception => {
          println(line)
          return None
        }
      }
    }
    return None
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")

    // Build up our vertices
    val users = sc.textFile("../data/users.csv").flatMap(parseUsers).persist()

    val usersIdScreenNames = sc.broadcast(users.map(u => ("@" + u.screen_name.toLowerCase, u.id)).collect().toMap)

    val tweets = sc.textFile("../data/tweets.csv").flatMap(parseTweet)

    def makeVerts(user: User): (VertexId, User) = (user.id, user)

    val verts = users.map(makeVerts)
    
    def makeEdges(tweet: Tweet): List[Edge[Int]] = {
      import scala.collection.mutable.ListBuffer
      var edges = new ListBuffer[Edge[Int]]()
      val fields = tweet.text.split(" ").filter(word => word.startsWith("@")).map(_.toLowerCase().replace(":", ""))
      val origin = tweet.user_id
      for (x <- 0 to (fields.length - 1)) {
        // Our attribute field is unused, but in other graphs could
        // be used to deep track of physical distances etc.
        val destin = usersIdScreenNames.value.get(fields(x))
        if (destin.isDefined)
          edges += Edge(origin.toLong, usersIdScreenNames.value.get(fields(x)).get, 0)
      }
      return edges.toList
    }

    val edges = tweets.flatMap(makeEdges)

    val default: User = null
    
    val graph = Graph(verts, edges, default).cache()

    val inDegrees = graph.inDegrees.join(verts).values.map(v => (v._2.screen_name, v._1)).distinct()
    
    val outDegrees = graph.outDegrees.join(verts).values.map(v => (v._2.screen_name, v._1)).distinct()
    
    val top10Influencer = inDegrees.sortBy(_._2, ascending = false).take(10)
    
    val top10Influenced = outDegrees.sortBy(_._2, ascending = false).take(10)
    
    println("\nTop 10 influencers: ")
    top10Influencer.foreach(u => println(u._1 + " is quoted by " + u._2))
    println("\nTop 10 influenced: ")
    top10Influenced.foreach(u => println(u._1 + " is influencied by " + u._2))

  }

}