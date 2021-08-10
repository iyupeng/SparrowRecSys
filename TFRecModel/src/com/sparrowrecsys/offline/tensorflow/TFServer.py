# tensorflow parameter server /worker.
# start with:
#  export TF_CONFIG=...
#  python TFServer.py


import os
import sys
import tensorflow as tf

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
os.environ["GRPC_FAIL_FAST"] = "use_caller"

if '.' not in sys.path:
    sys.path.insert(0, '.')

print("cluster config: " + os.environ["TF_CONFIG"])

cluster_resolver = tf.distribute.cluster_resolver.TFConfigClusterResolver()

server = tf.distribute.Server(
    cluster_resolver.cluster_spec(),
    job_name=cluster_resolver.task_type,
    task_index=cluster_resolver.task_id,
    protocol=cluster_resolver.rpc_layer or "grpc",
    start=True)

server.join()
