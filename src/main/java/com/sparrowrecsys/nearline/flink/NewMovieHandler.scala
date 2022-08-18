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

import scala.collection.{JavaConversions, mutable}

/**
 * Flink job to handle new movie message from Kafka.
 */
object NewMovieHandler {

  val redisEndpoint: String = Config.REDIS_SERVER
  val redisPort: Int = Config.REDIS_PORT
  val movieFeaturePrefix: String = Config.REDIS_KEY_PREFIX_MOVIE_FEATURE

  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(20000)

    // get input data
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", Config.KAFKA_BROKER_SERVERS)
    properties.setProperty("group.id", "sparrow_recsys")

    val movieStream = env
      .addSource(
        new FlinkKafkaConsumer[String](KafkaMessaging.NEW_MOVIE_TOPIC_NAME, new SimpleStringSchema(), properties)
          .setStartFromLatest())

    // processing
    movieStream
      .map(v => saveNewMovieFeaturesToRedis(v))

    // save to HDFS
    val sink: FileSink[String] = FileSink
      .forRowFormat(new Path(Config.HDFS_PATH_ALL_MOVIES), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()

    movieStream
      .sinkTo(sink)

    env.execute("New Movie Handler")
  }

  def saveNewMovieFeaturesToRedis(movie: String): String = {
    println(s"new movie: $movie")
    val movie_info = movie.split("\t")
    if (movie_info.length == 3) {
      val movieId = movie_info(0)
      val movieTitle = extractMovieTitle(movie_info(1))
      val releaseYear = extractReleaseYear(movie_info(1))
      val genres = movie_info(2).split("\\|")
      val movieGenre1 = if (genres.nonEmpty) genres(0) else ""
      val movieGenre2 = if (genres.length > 1) genres(1) else ""
      val movieGenre3 = if (genres.length > 2) genres(2) else ""

      val redisClient = new Jedis(redisEndpoint, redisPort)
      val params = SetParams.setParams()
      //set ttl to 24hs * 30
      params.ex(60 * 60 * 24 * 30)

      val movieKey = movieFeaturePrefix + ":" + movieId
      val valueMap = mutable.Map[String, String]()
      valueMap("movieGenre1") = movieGenre1
      valueMap("movieGenre2") = movieGenre2
      valueMap("movieGenre3") = movieGenre3
      valueMap("movieRatingCount") = "0"
      valueMap("releaseYear") = releaseYear.toString
      valueMap("movieAvgRating") = "0"
      valueMap("movieRatingStddev") = "0"

      val map = JavaConversions.mapAsJavaMap(valueMap)
      redisClient.hmset(movieKey, map)

      redisClient.close()
      println(s"movie features saved to redis with key: $movieKey")
    }

    movie
  }

  def extractReleaseYear(title: String): Int = {
    val reg = ".*\\(\\d{4}\\)$".r

    val tempTitle = title.stripPrefix("\"").stripSuffix("\"").trim
    tempTitle match {
      case reg() =>
        val yearString = tempTitle.substring(tempTitle.length - 5, tempTitle.length - 1)
        yearString.toInt
      case _ => 1990
    }
  }

  def extractMovieTitle(title: String): String = {
    val reg = ".*\\(\\d{4}\\)$".r


    val tempTitle = title.stripPrefix("\"").stripSuffix("\"").trim
    tempTitle match {
      case reg() => tempTitle.substring(0, tempTitle.length - 6).trim
      case _ => tempTitle
    }
  }
}
