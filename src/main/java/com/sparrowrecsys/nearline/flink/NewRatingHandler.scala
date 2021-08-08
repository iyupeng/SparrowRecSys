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

/**
 * Flink job to handle new rating message from Kafka.
 */
object NewRatingHandler {
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
          .setStartFromEarliest())

    // processing
    ratingStream
      .map(v => println(s"new rating: $v"))

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
}
