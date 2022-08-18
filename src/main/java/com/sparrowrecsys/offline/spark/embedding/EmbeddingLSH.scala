package com.sparrowrecsys.offline.spark.embedding

import java.text.SimpleDateFormat
import java.util.Date

import com.sparrowrecsys.online.datamanager.RedisClient
import com.sparrowrecsys.online.util.Config
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * Load movies embeddings from HDFS,
 * load user ratings from HDFS,
 * compute user embeddings,
 * compute Locality Sensitive Hashing,
 * and then save all to redis.
 */
object EmbeddingLSH {
  val LSH_BUCKET_LENGTH = 0.1
  val LSH_NUM_HASH_TABLES = 3

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("EmbeddingLSH")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val version = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date)
//    val version = "20210808202050"
    println("Embedding version: " + version)

    val movieEmbeddings = loadMovieEmbeddingsFromHDFS(spark)
    val userRatings = loadUserRatingsFromHDFS(spark)
    val userEmbeddings = computeUserEmbeddings(spark, movieEmbeddings, userRatings)
    saveUserEmbeddingsToRedis(spark, userEmbeddings, version)

    val movieUserEmbeddings = unionMovieAndUserEmbeddings(movieEmbeddings, userEmbeddings)
    val movieUserEmbeddingLSH = embeddingLSH(spark, movieUserEmbeddings)
    saveUserEmbeddingLSHToRedis(spark, movieUserEmbeddingLSH, version)
    saveMovieEmbeddingLSHToRedis(spark, movieUserEmbeddingLSH, version)
    saveLSHBucketMoviesToRedis(spark, movieUserEmbeddingLSH, version)
    updateEmbeddingVersions(version)
  }

  def loadMovieEmbeddingsFromHDFS(spark: SparkSession): DataFrame = {
    val file = spark.sparkContext.textFile(Config.HDFS_PATH_ALL_MOVIE_EMBEDDINGS + "*/part-*")

    val movieEmb =
      file
        .map{ line =>
          val fields = line.split("\t")
          if (fields.length == 2)
            Some((fields(0), Vectors.dense(fields(1).split(",").map(f => f.toDouble))))
          else
            None
        }
        .filter(_.nonEmpty)
        .map(_.get)

    val df = spark.createDataFrame(movieEmb).toDF("movieId", "emb")
    println("movie embeddings schema:")
    df.printSchema()
    df
  }

  def loadUserRatingsFromHDFS(spark: SparkSession): DataFrame = {
    val file = spark.sparkContext.textFile(Config.HDFS_PATH_ALL_RATINGS + "*/part-*")

    val userRatings =
      file
        .map{ line =>
          val fields = line.split("\t")
          if (fields.length == 4 && fields(2).toFloat > 3.5)
            Some((fields(0), fields(1), fields(2), fields(3)))
          else
            None
        }
        .filter(_.nonEmpty)
        .map(_.get)

    val df = spark.createDataFrame(userRatings).toDF("userId", "movieId", "rating", "time")
    println("user ratings schema:")
    df.printSchema()
    df
  }

  def computeUserEmbeddings(spark: SparkSession, movieEmb: DataFrame, userRatings: DataFrame): DataFrame = {
    import spark.implicits._

    val userEmbeddings = userRatings.sort(desc("time"))
      .dropDuplicates("userId", "movieId")
      .join(movieEmb, userRatings("movieId") === movieEmb("movieId"), "inner")
      .select(userRatings("userId"), movieEmb("emb") as "movieEmbedding")
      .groupBy("userId")
      .agg(
        Summarizer.mean($"movieEmbedding") as "emb",
        count(lit(1)) as "movieCount")
      .select("userId", "emb", "movieCount")

    userEmbeddings.printSchema()
    userEmbeddings.show(10)
    userEmbeddings
  }

  def saveUserEmbeddingsToRedis(spark:SparkSession, userEmbeddings: DataFrame, version: String): Unit = {
    userEmbeddings.foreachPartition(
      rows => {
        val keyValues = rows.flatMap(r => {
          val id = r.getAs[String]("userId")
          val count = r.getAs[Long]("movieCount")
          val embedding = r.getAs[DenseVector]("emb")
          Seq(s"${Config.REDIS_KEY_PREFIX_USER_EMBEDDING}:$version:$id", s"$count|${embedding.values.mkString(",")}")
        }).toArray
        if (keyValues.length > 0) {
          RedisClient.getInstance().mset(keyValues: _*)
        }
      }
    )
  }

  def unionMovieAndUserEmbeddings(movieEmb: DataFrame, userEmb: DataFrame): DataFrame = {
    movieEmb.withColumn("userId", lit(null).cast(StringType))
      .select("movieId", "userId", "emb")
      .union(
        userEmb.withColumn("movieId", lit(null).cast(StringType))
          .select("movieId", "userId", "emb")
      )
  }

  def embeddingLSH(spark:SparkSession, movieUserEmbDF: DataFrame): DataFrame = {
    //LSH bucket model
    val bucketProjectionLSH = new BucketedRandomProjectionLSH()
      .setBucketLength(LSH_BUCKET_LENGTH)
      .setNumHashTables(LSH_NUM_HASH_TABLES)
      .setInputCol("emb")
      .setOutputCol("bucketIds")

    val vectorsToArray: UserDefinedFunction = udf((vectors: Seq[DenseVector]) => {
      vectors.map(v => v(0)).zipWithIndex.map{case (e, i) => s"${i}_$e"}
    })

    val bucketModel = bucketProjectionLSH.fit(movieUserEmbDF)
    val embBucketResult = bucketModel.transform(movieUserEmbDF)
      .withColumn("bucketIds", vectorsToArray(col("bucketIds")))
    println("movieId, userId, emb, bucketIds schema:")
    embBucketResult.printSchema()
    println("movieId, userId, emb, bucketIds data result:")
    embBucketResult.show(10, truncate = false)

    embBucketResult
  }

  def saveUserEmbeddingLSHToRedis(spark:SparkSession, movieUserEmbeddingLSH: DataFrame, version: String): Unit = {
    saveEmbeddingLSHBucketsToRedis(
      spark,
      movieUserEmbeddingLSH,
      version,
      "userId",
      Config.REDIS_KEY_PREFIX_LSH_USER_BUCKETS)
  }

  def saveMovieEmbeddingLSHToRedis(spark:SparkSession, movieUserEmbeddingLSH: DataFrame, version: String): Unit = {
    saveEmbeddingLSHBucketsToRedis(
      spark,
      movieUserEmbeddingLSH,
      version,
      "movieId",
      Config.REDIS_KEY_PREFIX_LSH_MOVIE_BUCKETS)
  }

  def saveEmbeddingLSHBucketsToRedis(spark:SparkSession,
                                     movieUserEmbeddingLSH: DataFrame,
                                     version: String,
                                     idColumnName: String,
                                     redisKeyPrefix: String): Unit = {

    // get buckets
    val buckets = movieUserEmbeddingLSH
      .filter(col(idColumnName).isNotNull)
      .select(idColumnName, "bucketIds")
    buckets.printSchema()
    buckets.show(10)

    // save buckets
    buckets.foreachPartition(
      rows => {
        val keyValues = rows.flatMap(r => {
          val id = r.getAs[String](idColumnName)
          val buckets = r.getAs[Seq[String]]("bucketIds")
          Seq(s"$redisKeyPrefix:$version:$id", buckets.mkString(","))
        }).toArray

        if (keyValues.length > 0) {
          RedisClient.getInstance().mset(keyValues: _*)
        }
      }
    )
  }

  def saveLSHBucketMoviesToRedis(spark:SparkSession, movieUserEmbeddingLSH: DataFrame, version: String): Unit = {
    import spark.implicits._

    // get inverted index
    val bucketMovies = movieUserEmbeddingLSH
      .filter(col("movieId").isNotNull)
      .withColumn("bucketId", explode($"bucketIds"))
      .select("bucketId", "movieId")
    bucketMovies.show(10)

    val bucketMovieIds = bucketMovies.groupBy("bucketId")
      .agg(collect_list("movieId") as "movieIds")
    bucketMovieIds.show(10)

    // save bucket movies
    bucketMovieIds.foreachPartition(
      buckets => {
        val keyValues = buckets.flatMap(r => {
          val bucketId = r.getAs[String]("bucketId")
          val movieIds = r.getAs[Seq[String]]("movieIds")
          Seq(s"${Config.REDIS_KEY_PREFIX_LSH_BUCKET_MOVIES}:$version:$bucketId", movieIds.mkString(","))
        }).toArray

        if (keyValues.length > 0) {
          RedisClient.getInstance().mset(keyValues: _*)
        }
      }
    )
  }

  def updateEmbeddingVersions(version: String): Unit = {
    // update version
    RedisClient.getInstance().set(Config.REDIS_KEY_USER_EMBEDDING_VERSION, version)

    RedisClient.getInstance().set(Config.REDIS_KEY_LSH_MOVIE_BUCKETS_VERSION, version)

    RedisClient.getInstance().set(Config.REDIS_KEY_LSH_USER_BUCKETS_VERSION, version)

    RedisClient.getInstance().set(Config.REDIS_KEY_LSH_BUCKET_MOVIES_VERSION, version)
  }
}
