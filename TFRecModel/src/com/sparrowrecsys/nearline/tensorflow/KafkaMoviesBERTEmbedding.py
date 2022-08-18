# Load movies from Kafka, generate embeddings of movie titles with BERT, then save embeddings to
# redis and HDFS.

import os
import subprocess
from time import localtime, strftime

import numpy as np
import redis

import tensorflow_hub as hub
import tensorflow_text as text

from kafka import KafkaConsumer

HDFS_PATH_MOVIE_EMBEDDINGS="hdfs://demo-recsys-data:8020/sparrow_recsys/movie-embeddings/"
HDFS_MOVIE_EMBEDDING_BATCH_SIZE=3
REDIS_SERVER="demo-recsys-data"
REDIS_PORT=6379
KAFKA_SERVERS="demo-recsys-data:9092"
REDIS_KEY_MOVIE_EMBEDDING_VERSION="sparrow_recsys:version:me"
REDIS_KEY_PREFIX_MOVIE_EMBEDDING="sparrow_recsys:me"

tfhub_handle_preprocess = "https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3"
tfhub_handle_encoder = "https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-4_H-128_A-2/2"

bert_preprocess_model = hub.KerasLayer(tfhub_handle_preprocess)
bert_model = hub.KerasLayer(tfhub_handle_encoder)

current_batch_size = 0


def process_movies(movies: []):
    # get embeddings
    movie_ids = list(map(lambda x: int(x.decode('utf-8')), movies[:, 0]))
    movie_titles = movies[:, 1]
    text_preprocessed = bert_preprocess_model(movie_titles)
    bert_results = bert_model(text_preprocessed)

    movie_embeddings = list(map(lambda embeddings: ','.join(list(map(lambda f: str(f), embeddings.numpy()))), bert_results["pooled_output"]))
    movie_embeddings = list(zip(movie_ids, movie_embeddings))
    # remove duplicates
    movie_embeddings = dict(sorted(dict(movie_embeddings).items()))
    movie_embeddings = list(movie_embeddings.items())

    # save to file
    tmp_file_name = 'kafka-movie-embeddings.csv'

    with open(tmp_file_name, 'a') as tmp_file:
        list(map(lambda x: tmp_file.write(f"{x[0]}\t{x[1]}\n"), movie_embeddings))
        tmp_file.close()

    # save to redis
    r = redis.Redis(host=REDIS_SERVER, port=REDIS_PORT)
    version = r.get(REDIS_KEY_MOVIE_EMBEDDING_VERSION)

    if version is None:
        version = strftime("%Y%m%d%H%M%S", localtime())
        r.set(REDIS_KEY_MOVIE_EMBEDDING_VERSION, version)
        print(f"Redis movie embedding version is updated to: {version}")
    else:
        version = version.decode('utf-8')
        print(f"Using existing movie embedding version in redis: {version}")

    movie_embeddings_redis = list(map(
        lambda x: (f"{REDIS_KEY_PREFIX_MOVIE_EMBEDDING}:{version}:{x[0]}", x[1]),
        movie_embeddings))

    r.mset(dict(movie_embeddings_redis))

    global current_batch_size
    current_batch_size += len(movies)

    if current_batch_size >= HDFS_MOVIE_EMBEDDING_BATCH_SIZE:
        # save to HDFS
        batch_id=strftime("%Y%m%d%H%M%S", localtime())
        if os.path.isfile(tmp_file_name):
            subprocess.Popen(["hadoop", "fs", "-mkdir", "-p",
                              f"{HDFS_PATH_MOVIE_EMBEDDINGS}{batch_id}/"], stdout=subprocess.PIPE).communicate()
            subprocess.Popen(["hadoop", "fs", "-put", f"./{tmp_file_name}",
                              f"{HDFS_PATH_MOVIE_EMBEDDINGS}{batch_id}/part-0"], stdout=subprocess.PIPE).communicate()

            print(f"{current_batch_size} movie embeddings are uploaded to HDFS: {HDFS_PATH_MOVIE_EMBEDDINGS}{batch_id}/")
            os.remove(tmp_file_name)
            current_batch_size = 0


# load movies from kafka
consumer = KafkaConsumer('sparrow-recsys-new-movie',
                         group_id='sparrow_recsys.bert',
                         bootstrap_servers=KAFKA_SERVERS,
                         auto_offset_reset='latest')

for msg in consumer:
    print(msg.value)
    movies = []
    movie_str = msg.value
    movie_info = movie_str.split(b"\t")
    if len(movie_info) == 3:
        movies.append(movie_info)
        movies = np.array(movies)
        process_movies(movies)
