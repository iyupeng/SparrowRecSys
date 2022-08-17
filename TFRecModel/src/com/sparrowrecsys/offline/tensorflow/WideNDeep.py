
import os
import subprocess
import shutil
from time import localtime, strftime
import redis
import json

import tensorflow as tf

HDFS_PATH_SAMPLE_DATA = "hdfs://demo-recsys-data:8020/sparrow_recsys/sampledata/"
HDFS_PATH_MODEL_DATA = "hdfs://demo-recsys-data:8020/sparrow_recsys/modeldata/"
REDIS_SERVER="demo-recsys-data"
REDIS_PORT=6379
REDIS_KEY_VERSION_MODEL_WIDE_DEEP = "sparrow_recsys:version:model_wd"

# download sampling data from HDFS
tmp_sample_dir = "tmp_sampledata"
tmp_model_dir = "tmp_model"

if os.path.exists(tmp_sample_dir):
    shutil.rmtree(tmp_sample_dir)

subprocess.Popen(["hadoop", "fs", "-get", HDFS_PATH_SAMPLE_DATA, tmp_sample_dir], stdout=subprocess.PIPE).communicate()

# load sample as tf dataset
def get_dataset(file_path):
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=12,
        label_name='label',
        na_value="0",
        num_epochs=1,
        ignore_errors=True)
    return dataset


# genre features vocabulary
genre_vocab = ['Film-Noir', 'Action', 'Adventure', 'Horror', 'Romance', 'War', 'Comedy', 'Western', 'Documentary',
               'Sci-Fi', 'Drama', 'Thriller',
               'Crime', 'Fantasy', 'Animation', 'IMAX', 'Mystery', 'Children', 'Musical']

GENRE_FEATURES = {
    'userGenre1': genre_vocab,
    'userGenre2': genre_vocab,
    'userGenre3': genre_vocab,
    'userGenre4': genre_vocab,
    'userGenre5': genre_vocab,
    'movieGenre1': genre_vocab,
    'movieGenre2': genre_vocab,
    'movieGenre3': genre_vocab
}

# all categorical features
categorical_columns = []
for feature, vocab in GENRE_FEATURES.items():
    cat_col = tf.feature_column.categorical_column_with_vocabulary_list(
        key=feature, vocabulary_list=vocab)
    emb_col = tf.feature_column.embedding_column(cat_col, 10)
    categorical_columns.append(emb_col)

# movie id embedding feature
movie_col = tf.feature_column.categorical_column_with_identity(key='movieId', num_buckets=2001)
movie_emb_col = tf.feature_column.embedding_column(movie_col, 10)
categorical_columns.append(movie_emb_col)

# user id embedding feature
user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=30001)
user_emb_col = tf.feature_column.embedding_column(user_col, 10)
categorical_columns.append(user_emb_col)

# all numerical features
numerical_columns = [tf.feature_column.numeric_column('releaseYear'),
                     tf.feature_column.numeric_column('movieRatingCount'),
                     tf.feature_column.numeric_column('movieAvgRating'),
                     tf.feature_column.numeric_column('movieRatingStddev'),
                     tf.feature_column.numeric_column('userRatingCount'),
                     tf.feature_column.numeric_column('userAvgRating'),
                     tf.feature_column.numeric_column('userRatingStddev')]

# cross feature between current movie and user historical movie
rated_movie = tf.feature_column.categorical_column_with_identity(key='userRatedMovie1', num_buckets=2001)
crossed_feature = tf.feature_column.indicator_column(tf.feature_column.crossed_column([movie_col, rated_movie], 10000))

# define input for keras model
inputs = {
    'movieAvgRating': tf.keras.layers.Input(name='movieAvgRating', shape=(), dtype='float32'),
    'movieRatingStddev': tf.keras.layers.Input(name='movieRatingStddev', shape=(), dtype='float32'),
    'movieRatingCount': tf.keras.layers.Input(name='movieRatingCount', shape=(), dtype='int32'),
    'userAvgRating': tf.keras.layers.Input(name='userAvgRating', shape=(), dtype='float32'),
    'userRatingStddev': tf.keras.layers.Input(name='userRatingStddev', shape=(), dtype='float32'),
    'userRatingCount': tf.keras.layers.Input(name='userRatingCount', shape=(), dtype='int32'),
    'releaseYear': tf.keras.layers.Input(name='releaseYear', shape=(), dtype='int32'),

    'movieId': tf.keras.layers.Input(name='movieId', shape=(), dtype='int32'),
    'userId': tf.keras.layers.Input(name='userId', shape=(), dtype='int32'),
    'userRatedMovie1': tf.keras.layers.Input(name='userRatedMovie1', shape=(), dtype='int32'),

    'userGenre1': tf.keras.layers.Input(name='userGenre1', shape=(), dtype='string'),
    'userGenre2': tf.keras.layers.Input(name='userGenre2', shape=(), dtype='string'),
    'userGenre3': tf.keras.layers.Input(name='userGenre3', shape=(), dtype='string'),
    'userGenre4': tf.keras.layers.Input(name='userGenre4', shape=(), dtype='string'),
    'userGenre5': tf.keras.layers.Input(name='userGenre5', shape=(), dtype='string'),
    'movieGenre1': tf.keras.layers.Input(name='movieGenre1', shape=(), dtype='string'),
    'movieGenre2': tf.keras.layers.Input(name='movieGenre2', shape=(), dtype='string'),
    'movieGenre3': tf.keras.layers.Input(name='movieGenre3', shape=(), dtype='string'),
}

# train model with parameter servers
num_ps = len(json.loads(os.environ["TF_CONFIG"])['cluster']['ps'])
print(f"parameter server count: {num_ps}")

cluster_resolver = tf.distribute.cluster_resolver.TFConfigClusterResolver()

variable_partitioner = (
    tf.distribute.experimental.partitioners.MinSizePartitioner(
        min_shard_bytes=(256 << 10),
        max_shards=num_ps))

strategy = tf.distribute.experimental.ParameterServerStrategy(
    cluster_resolver,
    variable_partitioner=variable_partitioner)

def dataset_fn(input_context):
    train_dataset = get_dataset(f"{tmp_sample_dir}/trainingSamples/*/part-*.csv")
    dataset = train_dataset
    dataset = dataset.repeat().shard(
        input_context.num_input_pipelines,
        input_context.input_pipeline_id)

    return dataset

dc = tf.keras.utils.experimental.DatasetCreator(dataset_fn)

with strategy.scope():
    # wide and deep model architecture
    # deep part for all input features
    deep = tf.keras.layers.DenseFeatures(numerical_columns + categorical_columns)(inputs)
    deep = tf.keras.layers.Dense(128, activation='relu')(deep)
    deep = tf.keras.layers.Dense(128, activation='relu')(deep)
    # wide part for cross feature
    wide = tf.keras.layers.DenseFeatures(crossed_feature)(inputs)
    both = tf.keras.layers.concatenate([deep, wide])
    output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(both)

    model = tf.keras.Model(inputs, output_layer)

    # compile the model, set loss function, optimizer and evaluation metrics
    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=['accuracy', tf.keras.metrics.AUC(curve='ROC'), tf.keras.metrics.AUC(curve='PR')])

# train the model
working_dir = '/tmp/my_working_dir'
log_dir = os.path.join(working_dir, 'log')
ckpt_filepath = os.path.join(working_dir, 'ckpt')
backup_dir = os.path.join(working_dir, 'backup')

callbacks = [
    tf.keras.callbacks.TensorBoard(log_dir=log_dir),
    tf.keras.callbacks.ModelCheckpoint(filepath=ckpt_filepath),
    tf.keras.callbacks.experimental.BackupAndRestore(backup_dir=backup_dir),
]

model.fit(dc, epochs=5, steps_per_epoch=1000, callbacks=callbacks)

model.summary()

# evaluate the model
eval_accuracy = tf.keras.metrics.Accuracy()

test_dataset = get_dataset(f"{tmp_sample_dir}/testSamples/*/part-*.csv")
for batch_data, labels in test_dataset:
    pred = model(batch_data, training=False)
    actual_pred = tf.cast(tf.greater(pred, 0.5), tf.int64)
    eval_accuracy.update_state(labels, actual_pred)

print ("Evaluation accuracy: %f" % eval_accuracy.result())

# print some predict results
predictions = model.predict(test_dataset)
for prediction, goodRating in zip(predictions[:12], list(test_dataset)[0][1][:12]):
    print("Predicted good rating: {:.2%}".format(prediction[0]),
          " | Actual rating label: ",
          ("Good Rating" if bool(goodRating) else "Bad Rating"))

# save model
version=strftime("%Y%m%d%H%M%S", localtime())
print(f"Saving model with version: {version}")

tf.keras.models.save_model(
    model,
    f"{tmp_model_dir}/widendeep/{version}",
    overwrite=True,
    include_optimizer=True,
    save_format=None,
    signatures=None,
    options=None
)

if os.path.exists(f"{tmp_model_dir}/widendeep/{version}"):
    subprocess.Popen(["hadoop", "fs", "-rm", "-r", f"{HDFS_PATH_MODEL_DATA}widendeep/{version}"], stdout=subprocess.PIPE).communicate()
    subprocess.Popen(["hadoop", "fs", "-mkdir", "-p", f"{HDFS_PATH_MODEL_DATA}widendeep/"], stdout=subprocess.PIPE).communicate()
    subprocess.Popen(["hadoop", "fs", "-put", f"{tmp_model_dir}/widendeep/{version}", f"{HDFS_PATH_MODEL_DATA}widendeep/"], stdout=subprocess.PIPE).communicate()
    print(f"WideNDeep model data is uploaded to HDFS: {HDFS_PATH_MODEL_DATA}widendeep/{version}")

    # update model version in redis
    r = redis.Redis(host=REDIS_SERVER, port=REDIS_PORT)
    r.set(REDIS_KEY_VERSION_MODEL_WIDE_DEEP, version)

