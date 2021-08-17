package com.sparrowrecsys.nearline.flink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.sparrowrecsys.online.datamanager.KafkaMessaging
import com.sparrowrecsys.online.util.Config
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.immutable.ListMap
import scala.collection.{JavaConversions, JavaConverters, mutable}

/**
 * Flink job to handle new rating message from Kafka.
 */
object NewRatingHandler {
  val redisEndpoint: String = Config.REDIS_SERVER
  val redisPort: Int = Config.REDIS_PORT
  val movieFeaturePrefix: String = Config.REDIS_KEY_PREFIX_MOVIE_FEATURE
  val userFeaturePrefix: String = Config.REDIS_KEY_PREFIX_USER_FEATURE

  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(20000)

    // get input data
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", Config.KAFKA_BROKER_SERVERS)
    properties.setProperty("group.id", "sparrow_recsys")

    val ratingStream = env
      .addSource(
        new FlinkKafkaConsumer[String](KafkaMessaging.NEW_RATING_TOPIC_NAME, new SimpleStringSchema(), properties)
          .setStartFromLatest())

    // processing
    ratingStream
      .map(v => {
        println(s"new rating: $v")
        v.split("\t")
      })
      .filter(_.length == 4)
      .map(v => updateMovieFeaturesInRedis(v))
      .map(v => updateUserFeaturesInRedis(v))

    // save to HDFS
    val sink: FileSink[String] = FileSink
      .forRowFormat(new Path(Config.HDFS_PATH_ALL_RATINGS), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()

    ratingStream
      .sinkTo(sink)

    env.execute("New Rating Handler")
  }

  def updateMovieFeaturesInRedis(movie_info: Seq[String]): Seq[String] = {
    val movieId = movie_info(1)
    val rating = movie_info(2).toFloat

    val redisClient = new Jedis(redisEndpoint, redisPort)
    val movieKey = movieFeaturePrefix + ":" + movieId
    val oldRatingCount = redisClient.hget(movieKey, "movieRatingCount")
    val oldAverageRating = redisClient.hget(movieKey, "movieAvgRating")

    if (oldRatingCount != null && oldRatingCount.nonEmpty
      && oldAverageRating != null && oldAverageRating.nonEmpty) {
      val newRatingCount = oldRatingCount.toInt + 1
      val newAverageRating = (oldAverageRating.toFloat * oldRatingCount.toInt + rating) / newRatingCount

      redisClient.hset(movieKey, "movieRatingCount", newRatingCount.toString)
      redisClient.hset(movieKey, "movieAvgRating", newAverageRating.toString)

      println(s"movie features updated with redis key: $movieKey")
    }

    redisClient.close()
    movie_info
  }

  def updateUserFeaturesInRedis(movie_info: Seq[String]): Seq[String] = {
    val userId = movie_info.head
    val movieId = movie_info(1)
    val rating = movie_info(2).toFloat

    if (rating > 3.5) {
      val redisClient = new Jedis(redisEndpoint, redisPort)
      val params = SetParams.setParams()
      //set ttl to 24hs * 30
      params.ex(60 * 60 * 24 * 30)
      val movieKey = movieFeaturePrefix + ":" + movieId
      val userKey = userFeaturePrefix + ":" + userId

      if (!redisClient.exists(movieKey)) {
        return movie_info
      }

      val movieFeatures = redisClient.hmget(movieKey, "releaseYear", "movieGenre1", "movieGenre2", "movieGenre3")
      val movieYear = movieFeatures.get(0)
      val releaseYear = if (movieYear != null && movieYear.nonEmpty) movieYear.toInt else 1990

      if (!redisClient.exists(userKey)) {
        val valueMap = mutable.Map[String, String]()
        valueMap("userId") = userId
        valueMap("userRatedMovie1") = movieId
        valueMap("userRatedMovie2") = "0"
        valueMap("userRatedMovie3") = "0"
        valueMap("userRatedMovie4") = "0"
        valueMap("userRatedMovie5") = "0"
        valueMap("userRatingCount") = "1"
        valueMap("userAvgReleaseYear") = releaseYear.toString
        valueMap("userAvgRating") = rating.toString
        valueMap("userGenre1") = movieFeatures.get(1)
        valueMap("userGenre2") = movieFeatures.get(2)
        valueMap("userGenre3") = movieFeatures.get(3)
        valueMap("userGenre4") = ""
        valueMap("userGenre5") = ""
        valueMap("userGenreCount1") = if (movieFeatures.get(1).length > 0) "1" else "0"
        valueMap("userGenreCount2") = if (movieFeatures.get(2).length > 0) "1" else "0"
        valueMap("userGenreCount3") = if (movieFeatures.get(3).length > 0) "1" else "0"
        valueMap("userGenreCount4") = "0"
        valueMap("userGenreCount5") = "0"

        val map = JavaConversions.mapAsJavaMap(valueMap)
        redisClient.hmset(userKey, map)

        println(s"user features saved to redis with key: $userKey")
      } else {
        val userFields = Seq("userId",
          "userRatedMovie1", "userRatedMovie2", "userRatedMovie3", "userRatedMovie4", "userRatedMovie5",
          "userRatingCount", "userAvgReleaseYear", "userAvgRating",
          "userGenre1", "userGenre2", "userGenre3", "userGenre4", "userGenre5",
          "userGenreCount1", "userGenreCount2","userGenreCount3","userGenreCount4","userGenreCount5")
        val userFeatures = redisClient.hmget(userKey, userFields: _*)
        userFeatures.set(userFields.toList.indexOf("userId"), userId)

        for (index <- 1 to 5) {
          val ratedMovie = userFeatures.get(userFields.toList.indexOf(s"userRatedMovie$index"))
          if (ratedMovie != null && ratedMovie.nonEmpty && ratedMovie.equals(movieId)) {

            println(s"user features not updated to redis with key: $userKey, reason: existed movie in feature.")
            return movie_info
          }
        }

        // recent movies
        userFeatures.set(userFields.toList.indexOf("userRatedMovie5"), userFeatures.get(userFields.toList.indexOf("userRatedMovie4")))
        userFeatures.set(userFields.toList.indexOf("userRatedMovie4"), userFeatures.get(userFields.toList.indexOf("userRatedMovie3")))
        userFeatures.set(userFields.toList.indexOf("userRatedMovie3"), userFeatures.get(userFields.toList.indexOf("userRatedMovie2")))
        userFeatures.set(userFields.toList.indexOf("userRatedMovie2"), userFeatures.get(userFields.toList.indexOf("userRatedMovie1")))
        userFeatures.set(userFields.toList.indexOf("userRatedMovie1"), movieId)

        val oldCount = userFeatures.get(userFields.toList.indexOf("userRatingCount"))
        val oldAvgYear = userFeatures.get(userFields.toList.indexOf("userAvgReleaseYear"))
        val oldAvgRating = userFeatures.get(userFields.toList.indexOf("userAvgRating"))

        // average rating, average year
        if (oldCount != null && oldCount.nonEmpty) {
          val newAvgYear = (oldAvgYear.toFloat * oldCount.toInt + releaseYear) / (oldCount.toInt + 1)
          userFeatures.set(userFields.toList.indexOf("userAvgReleaseYear"), newAvgYear.toString)

          val newAvgRating = (oldAvgRating.toFloat * oldCount.toInt + rating) / (oldCount.toInt + 1)
          userFeatures.set(userFields.toList.indexOf("userAvgRating"), newAvgRating.toString)

          userFeatures.set(userFields.toList.indexOf("userRatingCount"), (oldCount.toInt + 1).toString)
        }

        // top genres
        val genreMap = mutable.Map[String, Int]()
        for (index <- 1 to 5) {
          genreMap(userFeatures.get(userFields.toList.indexOf(s"userGenre$index")))
            = userFeatures.get(userFields.toList.indexOf(s"userGenreCount$index")).toInt
        }
        for (index <- 1 to 3) {
          if (movieFeatures.get(index).length > 0) {
            genreMap(movieFeatures.get(index)) = genreMap.getOrElse[Int](movieFeatures.get(index), 0)  + 1
          }
        }
        val sortedGenres = ListMap(genreMap.toSeq.sortWith(_._2 > _._2):_*).toList
        for (index <- 1 to 5) {
          if (sortedGenres.length >= index) {
            userFeatures.set(userFields.toList.indexOf(s"userGenre$index"), sortedGenres(index - 1)._1)
            userFeatures.set(userFields.toList.indexOf(s"userGenreCount$index"), sortedGenres(index - 1)._2.toString)
          } else {
            userFeatures.set(userFields.toList.indexOf(s"userGenre$index"), "")
            userFeatures.set(userFields.toList.indexOf(s"userGenreCount$index"), "0")
          }
        }

        // save to redis
        val keyValues = userFields.zip(JavaConverters.asScalaIteratorConverter(userFeatures.iterator()).asScala.toSeq)
        val valueMap = mutable.Map[String, String]()
        for ((key, value) <- keyValues) {
          valueMap(key) = value
        }
        val map = JavaConversions.mapAsJavaMap(valueMap)
        redisClient.hmset(userKey, map)

        redisClient.close()
        println(s"user features updated to redis with key: $userKey")
      }
    }

    movie_info
  }
}
