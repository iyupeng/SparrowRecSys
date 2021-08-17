package com.sparrowrecsys.online.datamanager;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static com.sparrowrecsys.online.util.Config.KAFKA_BROKER_SERVERS;

/**
 * Utils for Kafka message sending.
 */
public class KafkaMessaging {
    public static String NEW_MOVIE_TOPIC_NAME = "sparrow-recsys-new-movie";
    public static String NEW_RATING_TOPIC_NAME = "sparrow-recsys-new-rating";

    protected static Producer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BROKER_SERVERS);
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    public static void sendNewMovie(Movie movie) {
        Producer<String, String> producer = getProducer();

        String message = String.format("%d\t%s\t%s",
                movie.getMovieId(),
                movie.getTitle() + " (" + movie.getReleaseYear() + ")",
                String.join("|", movie.getGenres()));
        producer.send(new ProducerRecord<>(NEW_MOVIE_TOPIC_NAME, String.valueOf(movie.getMovieId()), message));
        System.out.println("new movie sent to kafka.");

        producer.close();
    }

    public static void sendNewRating(Rating rating) {
        Producer<String, String> producer = getProducer();

        String message = String.format("%d\t%d\t%f\t%d", rating.getUserId(), rating.getMovieId(), rating.getScore(), rating.getTimestamp());
        producer.send(new ProducerRecord<>(NEW_RATING_TOPIC_NAME, String.valueOf(rating.getUserId()), message));
        System.out.println("new rating sent to kafka.");

        producer.close();
    }
}
