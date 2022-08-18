package com.sparrowrecsys.offline.spark.featureeng

import java.nio.file.{Files, Paths, StandardCopyOption}

import com.sparrowrecsys.online.util.Config
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{format_number, _}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.immutable.ListMap
import scala.collection.{JavaConversions, mutable}

object FeatureEngForRecModel {

  val NUMBER_PRECISION = 2
  val redisEndpoint: String = Config.REDIS_SERVER
  val redisPort: Int = Config.REDIS_PORT

  def addSampleLabel(ratingSamples:DataFrame): DataFrame ={
    ratingSamples.show(10, truncate = false)
    ratingSamples.printSchema()
    val sampleCount = ratingSamples.count()
    ratingSamples.groupBy(col("rating")).count().orderBy(col("rating"))
      .withColumn("percentage", col("count")/sampleCount).show(100,truncate = false)

    ratingSamples.withColumn("label", when(col("rating") >= 3.5, 1).otherwise(0))
  }

  def addMovieFeatures(movieSamples:DataFrame, ratingSamples:DataFrame): DataFrame ={

    //add movie basic features
    val samplesWithMovies1 = ratingSamples.join(movieSamples, Seq("movieId"), "left")
    //add release year
    val extractReleaseYearUdf = udf({(title: String) => {
      if (null == title || title.stripPrefix("\"").stripSuffix("\"").trim.length < 6) {
        1990 // default value
      }
      else {
        val tempTitle = title.stripPrefix("\"").stripSuffix("\"").trim
        val yearString = tempTitle.substring(tempTitle.length - 5, tempTitle.length - 1)
        try {
          yearString.toInt
        } catch {
          case _: Throwable => 1990 // default value
        }
      }
    }})

    //add title
    val extractTitleUdf = udf({(title: String) => {
      val reg = ".*\\(\\d{4}\\)$".r

      val tempTitle = title.stripPrefix("\"").stripSuffix("\"").trim
      tempTitle match {
        case reg() => tempTitle.substring(0, tempTitle.trim.length - 6).trim
        case _ => tempTitle
      }
    }})

    val samplesWithMovies2 = samplesWithMovies1.withColumn("releaseYear", extractReleaseYearUdf(col("title")))
      .withColumn("title", extractTitleUdf(col("title")))
      .drop("title")  //title is useless currently

    //split genres
    val samplesWithMovies3 = samplesWithMovies2.withColumn("movieGenre1",split(col("genres"),"\\|").getItem(0))
      .withColumn("movieGenre2",split(col("genres"),"\\|").getItem(1))
      .withColumn("movieGenre3",split(col("genres"),"\\|").getItem(2))

    //add rating features
    val movieRatingFeatures = samplesWithMovies3.groupBy(col("movieId"))
      .agg(count(lit(1)).as("movieRatingCount"),
        format_number(avg(col("rating")), NUMBER_PRECISION).as("movieAvgRating"),
        stddev(col("rating")).as("movieRatingStddev"))
    .na.fill(0).withColumn("movieRatingStddev",format_number(col("movieRatingStddev"), NUMBER_PRECISION))


    //join movie rating features
    val samplesWithMovies4 = samplesWithMovies3.join(movieRatingFeatures, Seq("movieId"), "left")
    samplesWithMovies4.printSchema()
    samplesWithMovies4.show(10, truncate = false)

    samplesWithMovies4
  }

  val extractGenres: UserDefinedFunction = udf { (genreArray: Seq[String]) => {
    val genreMap = mutable.Map[String, Int]()
    genreArray.foreach((element:String) => {
      val genres = element.split("\\|")
      genres.foreach((oneGenre:String) => {
        genreMap(oneGenre) = genreMap.getOrElse[Int](oneGenre, 0)  + 1
      })
    })
    val sortedGenres = ListMap(genreMap.toSeq.sortWith(_._2 > _._2):_*)
    sortedGenres.keys.toSeq
  }}

  val extractGenresCount: UserDefinedFunction = udf { (genreArray: Seq[String]) => {
    val genreMap = mutable.Map[String, Int]()
    genreArray.foreach((element:String) => {
      val genres = element.split("\\|")
      genres.foreach((oneGenre:String) => {
        genreMap(oneGenre) = genreMap.getOrElse[Int](oneGenre, 0)  + 1
      })
    })
    val sortedGenres = ListMap(genreMap.toSeq.sortWith(_._2 > _._2):_*)
    sortedGenres.values.toSeq
  }}

  def addUserFeatures(ratingSamples:DataFrame): DataFrame ={
    val samplesWithUserFeatures = ratingSamples
      .withColumn("userPositiveHistory", collect_list(when(col("label") === 1, col("movieId")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userPositiveHistory", reverse(col("userPositiveHistory")))
      .withColumn("userRatedMovie1",col("userPositiveHistory").getItem(0))
      .withColumn("userRatedMovie2",col("userPositiveHistory").getItem(1))
      .withColumn("userRatedMovie3",col("userPositiveHistory").getItem(2))
      .withColumn("userRatedMovie4",col("userPositiveHistory").getItem(3))
      .withColumn("userRatedMovie5",col("userPositiveHistory").getItem(4))
      .withColumn("userRatingCount", count(lit(1))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userAvgReleaseYear", avg(col("releaseYear"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)).cast(IntegerType))
      .withColumn("userReleaseYearStddev", stddev(col("releaseYear"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userAvgRating", format_number(avg(col("rating"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)), NUMBER_PRECISION))
      .withColumn("userRatingStddev", stddev(col("rating"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userGenres", extractGenres(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1))))
      .withColumn("userGenresCount", extractGenresCount(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1))))
      .withColumn("userGenreCount1",col("userGenresCount").getItem(0))
      .withColumn("userGenreCount2",col("userGenresCount").getItem(1))
      .withColumn("userGenreCount3",col("userGenresCount").getItem(2))
      .withColumn("userGenreCount4",col("userGenresCount").getItem(3))
      .withColumn("userGenreCount5",col("userGenresCount").getItem(4))
      .na.fill(0)
      .withColumn("userRatingStddev",format_number(col("userRatingStddev"), NUMBER_PRECISION))
      .withColumn("userReleaseYearStddev",format_number(col("userReleaseYearStddev"), NUMBER_PRECISION))
      .withColumn("userGenre1",col("userGenres").getItem(0))
      .withColumn("userGenre2",col("userGenres").getItem(1))
      .withColumn("userGenre3",col("userGenres").getItem(2))
      .withColumn("userGenre4",col("userGenres").getItem(3))
      .withColumn("userGenre5",col("userGenres").getItem(4))
      .drop("genres", "userGenres", "userGenresCount", "userPositiveHistory")
      .filter(col("userRatingCount") > 1)

    samplesWithUserFeatures.printSchema()
    samplesWithUserFeatures.show(10, truncate = false)

    samplesWithUserFeatures
  }

  def extractAndSaveMovieFeaturesToRedis(samples:DataFrame): DataFrame = {
    val movieLatestSamples = samples.withColumn("movieRowNum", row_number()
      .over(Window.partitionBy("movieId")
        .orderBy(col("timestamp").desc)))
      .filter(col("movieRowNum") === 1)
      .select("movieId","releaseYear", "movieGenre1","movieGenre2","movieGenre3","movieRatingCount",
        "movieAvgRating", "movieRatingStddev")
      .na.fill("")

    movieLatestSamples.printSchema()
    movieLatestSamples.show(20, truncate = false)

    val movieFeaturePrefix = Config.REDIS_KEY_PREFIX_MOVIE_FEATURE

    val redisClient = new Jedis(redisEndpoint, redisPort)
    val params = SetParams.setParams()
    //set ttl to 24hs * 30
    params.ex(60 * 60 * 24 * 30)
    val sampleArray = movieLatestSamples.collect()
    println("total movie size:" + sampleArray.length)

    var insertedMovieNumber = 0
    val movieCount = sampleArray.length
    for (sample <- sampleArray){
      val movieKey = movieFeaturePrefix + ":" + sample.getAs[String]("movieId")
      val valueMap = mutable.Map[String, String]()
      valueMap("movieGenre1") = sample.getAs[String]("movieGenre1")
      valueMap("movieGenre2") = sample.getAs[String]("movieGenre2")
      valueMap("movieGenre3") = sample.getAs[String]("movieGenre3")
      valueMap("movieRatingCount") = sample.getAs[Long]("movieRatingCount").toString
      valueMap("releaseYear") = sample.getAs[Int]("releaseYear").toString
      valueMap("movieAvgRating") = sample.getAs[String]("movieAvgRating")
      valueMap("movieRatingStddev") = sample.getAs[String]("movieRatingStddev")

      val map = JavaConversions.mapAsJavaMap(valueMap)
      redisClient.hmset(movieKey, map)
      insertedMovieNumber += 1
      if (insertedMovieNumber % 100 ==0){
        println(insertedMovieNumber + "/" + movieCount + "...")
      }
    }

    redisClient.close()
    movieLatestSamples
  }

  def splitAndSaveTrainingTestSamples(samples:DataFrame, savePath:String): Unit ={
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.5)

    //split training and test set by 8:2
    val Array(training, test) = smallSamples.randomSplit(Array(0.8, 0.2))

    val sampleResourcesPath = this.getClass.getResource(savePath)
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(savePath+"/trainingSamples/0000")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(savePath+"/testSamples/0000")
  }

  def splitAndSaveTrainingTestSamplesByTimeStamp(samples:DataFrame, savePath:String): Unit ={
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.1).withColumn("timestampLong", col("timestamp").cast(LongType))

    val quantile = smallSamples.stat.approxQuantile("timestampLong", Array(0.8), 0.05)
    val splitTimestamp = quantile.apply(0)

    val training = smallSamples.where(col("timestampLong") <= splitTimestamp).drop("timestampLong")
    val test = smallSamples.where(col("timestampLong") > splitTimestamp).drop("timestampLong")

    val sampleResourcesPath = this.getClass.getResource(savePath)
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/trainingSamples")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/testSamples")
  }


  def extractAndSaveUserFeaturesToRedis(samples:DataFrame): DataFrame = {
    val userLatestSamples = samples.withColumn("userRowNum", row_number()
      .over(Window.partitionBy("userId")
        .orderBy(col("timestamp").desc)))
      .filter(col("userRowNum") === 1)
      .select("userId","userRatedMovie1", "userRatedMovie2","userRatedMovie3","userRatedMovie4","userRatedMovie5",
        "userRatingCount", "userAvgReleaseYear", "userReleaseYearStddev", "userAvgRating", "userRatingStddev",
        "userGenre1", "userGenre2","userGenre3","userGenre4","userGenre5",
        "userGenreCount1", "userGenreCount2","userGenreCount3","userGenreCount4","userGenreCount5")
      .na.fill("")

    userLatestSamples.printSchema()
    userLatestSamples.show(20, truncate = false)

    val userFeaturePrefix = Config.REDIS_KEY_PREFIX_USER_FEATURE

    val redisClient = new Jedis(redisEndpoint, redisPort)
    val params = SetParams.setParams()
    //set ttl to 24hs * 30
    params.ex(60 * 60 * 24 * 30)
    val sampleArray = userLatestSamples.collect()
    println("total user size:" + sampleArray.length)

    var insertedUserNumber = 0
    val userCount = sampleArray.length
    for (sample <- sampleArray){
      val userKey = userFeaturePrefix + ":" + sample.getAs[String]("userId")
      val valueMap = mutable.Map[String, String]()
      valueMap("userRatedMovie1") = sample.getAs[String]("userRatedMovie1")
      valueMap("userRatedMovie2") = sample.getAs[String]("userRatedMovie2")
      valueMap("userRatedMovie3") = sample.getAs[String]("userRatedMovie3")
      valueMap("userRatedMovie4") = sample.getAs[String]("userRatedMovie4")
      valueMap("userRatedMovie5") = sample.getAs[String]("userRatedMovie5")
      valueMap("userGenre1") = sample.getAs[String]("userGenre1")
      valueMap("userGenre2") = sample.getAs[String]("userGenre2")
      valueMap("userGenre3") = sample.getAs[String]("userGenre3")
      valueMap("userGenre4") = sample.getAs[String]("userGenre4")
      valueMap("userGenre5") = sample.getAs[String]("userGenre5")
      valueMap("userGenreCount1") = sample.getAs[Int]("userGenreCount1").toString
      valueMap("userGenreCount2") = sample.getAs[Int]("userGenreCount2").toString
      valueMap("userGenreCount3") = sample.getAs[Int]("userGenreCount3").toString
      valueMap("userGenreCount4") = sample.getAs[Int]("userGenreCount4").toString
      valueMap("userGenreCount5") = sample.getAs[Int]("userGenreCount5").toString
      valueMap("userRatingCount") = sample.getAs[Long]("userRatingCount").toString
      valueMap("userAvgReleaseYear") = sample.getAs[Int]("userAvgReleaseYear").toString
      valueMap("userReleaseYearStddev") = sample.getAs[String]("userReleaseYearStddev")
      valueMap("userAvgRating") = sample.getAs[String]("userAvgRating")
      valueMap("userRatingStddev") = sample.getAs[String]("userRatingStddev")

      val map = JavaConversions.mapAsJavaMap(valueMap)
      redisClient.hmset(userKey, map)
      insertedUserNumber += 1
      if (insertedUserNumber % 100 ==0){
        println(insertedUserNumber + "/" + userCount + "...")
      }
    }

    redisClient.close()
    userLatestSamples
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("FeatureEngineering")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val movieSamples = loadMoviesFromHDFS(spark)
    val ratingSamples = loadRatingsFromHDFS(spark)

    val ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    ratingSamplesWithLabel.show(10, truncate = false)

    val samplesWithMovieFeatures = addMovieFeatures(movieSamples, ratingSamplesWithLabel)
    //save item features to redis for online inference
    extractAndSaveMovieFeaturesToRedis(samplesWithMovieFeatures)

    val samplesWithUserFeatures = addUserFeatures(samplesWithMovieFeatures)
    //save user features to redis for online inference
    extractAndSaveUserFeaturesToRedis(samplesWithUserFeatures)

    //save samples as csv format
    splitAndSaveTrainingTestSamples(samplesWithUserFeatures, Config.HDFS_PATH_SAMPLE_DATA)

    spark.close()
  }

  def loadMoviesFromHDFS(spark: SparkSession): DataFrame = {
    val file = spark.sparkContext.textFile(Config.HDFS_PATH_ALL_MOVIES + "*/part-*")

    val data =
      file
        .map{ line =>
          val fields = line.split("\t")
          if (fields.length == 3)
            Some((fields(0), fields(1), fields(2)))
          else
            None
        }
        .filter(_.nonEmpty)
        .map(_.get)

    val df = spark.createDataFrame(data).toDF("movieId", "title", "genres")
    println("movies schema:")
    df.printSchema()
    df
  }

  def loadRatingsFromHDFS(spark: SparkSession): DataFrame = {
    val file = spark.sparkContext.textFile(Config.HDFS_PATH_ALL_RATINGS + "*/part-*")

    val data =
      file
        .map{ line =>
          val fields = line.split("\t")
          if (fields.length == 4)
            Some((fields(0), fields(1), fields(2), fields(3)))
          else
            None
        }
        .filter(_.nonEmpty)
        .map(_.get)

    val df = spark.createDataFrame(data)
      .toDF("userId", "movieId", "rating", "timestamp")
      .withColumn("userMovieRowNum", row_number()
        .over(Window.partitionBy("userId", "movieId")
          .orderBy(col("timestamp").desc)))
      .filter(col("userMovieRowNum") === 1)
      .drop("userMovieRowNum")
      .orderBy("timestamp")

    println("user ratings schema:")
    df.printSchema()
    df
  }

  def resourceToLocal(resourcePath: String): String = {
    val outPath = Paths.get("/tmp/" + resourcePath)
    val resourceFileStream = getClass.getResourceAsStream(resourcePath)
    Files.createDirectories(outPath.getParent)
    Files.copy(resourceFileStream, outPath, StandardCopyOption.REPLACE_EXISTING)
    resourceFileStream.close()
    val ret = s"file://${outPath.toUri.getPath}"
    println(ret)
    ret
  }
}
