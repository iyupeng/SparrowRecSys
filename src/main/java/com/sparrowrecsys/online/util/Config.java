package com.sparrowrecsys.online.util;

public class Config {
    public static final String DATA_SOURCE_REDIS = "redis";
    public static final String DATA_SOURCE_FILE = "file";

    public static String EMB_DATA_SOURCE = Config.DATA_SOURCE_FILE;
    public static boolean IS_LOAD_USER_FEATURE_FROM_REDIS = true;
    public static boolean IS_LOAD_ITEM_FEATURE_FROM_REDIS = false;

    public static boolean IS_ENABLE_AB_TEST = false;

    public static String MYSQL_DB_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/sparrow_recsys?" +
            "user=sparrow&password=Recsys1$&autoReconnect=true";

    public static String REDIS_SERVER = "localhost";
    public static int REDIS_PORT = 6379;

    public static String KAFKA_BROKER_SERVERS = "localhost:9092";
    public static String HDFS_PATH_ALL_MOVIES = "hdfs:///sparrow_recsys/movies/";
    public static String HDFS_PATH_ALL_RATINGS = "hdfs:///sparrow_recsys/ratings/";
    public static String HDFS_PATH_SAMPLE_DATA = "hdfs:///sparrow_recsys/sampledata/";
    public static String HDFS_PATH_MODEL_DATA = "hdfs:///sparrow_recsys/modeldata/";

    public static String REDIS_KEY_VERSION_MODEL_WIDE_DEEP = "sparrow_recsys:version:model_wd";
    public static String TF_SERVING_URL_PREFIX_WIDE_DEEP =
            "http://localhost:8501/v1/models/sparrow_recsys_widedeep";

    public static String HDFS_PATH_ALL_MOVIE_EMBEDDINGS = "hdfs:///sparrow_recsys/movie-embeddings/";

    public static String REDIS_KEY_USER_EMBEDDING_VERSION="sparrow_recsys:version:ue";
    public static String REDIS_KEY_PREFIX_USER_EMBEDDING="sparrow_recsys:ue";

    public static String REDIS_KEY_MOVIE_EMBEDDING_VERSION="sparrow_recsys:version:me";
    public static String REDIS_KEY_PREFIX_MOVIE_EMBEDDING="sparrow_recsys:me";

    public static String REDIS_KEY_LSH_MOVIE_BUCKETS_VERSION="sparrow_recsys:version:lsh_movie_buckets";
    public static String REDIS_KEY_PREFIX_LSH_MOVIE_BUCKETS="sparrow_recsys:lsh_movie_buckets";

    public static String REDIS_KEY_LSH_USER_BUCKETS_VERSION="sparrow_recsys:version:lsh_user_buckets";
    public static String REDIS_KEY_PREFIX_LSH_USER_BUCKETS="sparrow_recsys:lsh_user_buckets";

    public static String REDIS_KEY_LSH_BUCKET_MOVIES_VERSION="sparrow_recsys:version:lsh_bucket_movies";
    public static String REDIS_KEY_PREFIX_LSH_BUCKET_MOVIES="sparrow_recsys:lsh_bucket_movies";

    public static String REDIS_KEY_PREFIX_USER_FEATURE="sparrow_recsys:uf";
    public static String REDIS_KEY_PREFIX_MOVIE_FEATURE="sparrow_recsys:mf";
}
