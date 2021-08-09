# Load movies from HDFS, generate embeddings of movie titles with BERT, then save embeddings to
# redis and HDFS.

import subprocess
from time import localtime, strftime

import numpy as np
import redis

import tensorflow_hub as hub
import tensorflow_text as text

import os


HDFS_PATH_MOVIE_EMBEDDINGS="hdfs:///sparrow_recsys/movie-embeddings/"
REDIS_SERVER="localhost"
REDIS_PORT=6379
REDIS_KEY_MOVIE_EMBEDDING_VERSION="sparrow_recsys:version:me"
REDIS_KEY_PREFIX_MOVIE_EMBEDDING="sparrow_recsys:me"

# load movies from HDFS
movies = []
cat_hdfs_movies = subprocess.Popen(["hadoop", "fs", "-cat", "hdfs:///sparrow_recsys/movies/*/part-*"], stdout=subprocess.PIPE)
for line in cat_hdfs_movies.stdout:
    movie_str = line.strip()
    movie_info = movie_str.split(b"\t")
    if len(movie_info) == 3:
        movies.append(movie_info)

movies = np.array(movies)
print(f"HDFS movies count: {len(movies)}, first: {movies[0]}")

if len(movies) == 0:
    exit(1)

# get embeddings
tfhub_handle_preprocess = "https://hub.tensorflow.google.cn/tensorflow/bert_en_uncased_preprocess/3"
tfhub_handle_encoder = "https://hub.tensorflow.google.cn/tensorflow/small_bert/bert_en_uncased_L-4_H-128_A-2/2"

bert_preprocess_model = hub.KerasLayer(tfhub_handle_preprocess)
bert_model = hub.KerasLayer(tfhub_handle_encoder)

movie_ids = list(map(lambda x: int(x.decode('utf-8')), movies[:, 0]))
movie_titles = movies[:, 1]
text_preprocessed = bert_preprocess_model(movie_titles)
bert_results = bert_model(text_preprocessed)

print(f'Pooled Outputs Shape: {bert_results["pooled_output"].shape}')
print(f'Pooled Outputs Values[0, :12]: {bert_results["pooled_output"][0, :12]}')

movie_embeddings = list(map(lambda embeddings: ','.join(list(map(lambda f: str(f), embeddings.numpy()))), bert_results["pooled_output"]))
movie_embeddings = list(zip(movie_ids, movie_embeddings))
# remove duplicates
movie_embeddings = dict(sorted(dict(movie_embeddings).items()))
movie_embeddings = list(movie_embeddings.items())

print(f"Movie embedding sample: {movie_embeddings[0]}")

# save to HDFS
tmp_file_name = 'movie-embeddings.csv'

if os.path.isfile(tmp_file_name):
    os.remove(tmp_file_name)

with open(tmp_file_name, 'a') as tmp_file:
    list(map(lambda x: tmp_file.write(f"{x[0]}\t{x[1]}\n"), movie_embeddings))

if os.path.isfile(tmp_file_name):
    subprocess.Popen(["hadoop", "fs", "-rm", "-r", HDFS_PATH_MOVIE_EMBEDDINGS], stdout=subprocess.PIPE).communicate()
    subprocess.Popen(["hadoop", "fs", "-mkdir", "-p", f"{HDFS_PATH_MOVIE_EMBEDDINGS}0000/"], stdout=subprocess.PIPE).communicate()
    subprocess.Popen(["hadoop", "fs", "-put", f"./{tmp_file_name}", f"{HDFS_PATH_MOVIE_EMBEDDINGS}0000/part-0"], stdout=subprocess.PIPE).communicate()
    os.remove(tmp_file_name)
    print(f"Movie embeddings is uploaded to HDFS: {HDFS_PATH_MOVIE_EMBEDDINGS}")

# save to redis
version=strftime("%Y%m%d%H%M%S", localtime())

movie_embeddings_redis = list(map(
    lambda x: (f"{REDIS_KEY_PREFIX_MOVIE_EMBEDDING}:{version}:{x[0]}", x[1]),
    movie_embeddings))

r = redis.Redis(host=REDIS_SERVER, port=REDIS_PORT)
r.mset(dict(movie_embeddings_redis))
r.set(REDIS_KEY_MOVIE_EMBEDDING_VERSION, version)

print(f"Movie embedding version is updated to: {version}")
