# SparrowRecSys
SparrowRecSys是一个电影推荐系统，名字SparrowRecSys（麻雀推荐系统），取自“麻雀虽小，五脏俱全”之意。项目是一个基于maven的混合语言项目，同时包含了TensorFlow，Spark，Jetty Server等推荐系统的不同模块。希望你能够利用SparrowRecSys进行推荐系统的学习，并有机会一起完善它。

## 环境要求
* Java 8
* Scala 2.11
* Python 3.6+
* TensorFlow 2.0+
* Mysql 5.6
* Redis
* Kafka
* Hadoop 2.9.2
* Spark 2.4.7+
* Flink 1.12.0
* Docker
* Python packages: `tensorflow, tensorflow_hub, tensorflow_text, redis, kafka-python`

## 启动步骤
### 编译
```
mvn clean package

```


### 导入初始数据
```

# 用 sql/db.sql 创建MySQL数据库

# 将movies和ratings数据导入MySQL (数据位于src/main/resources/webroot/sampledata/)

# 将MySQL数据导入HDFS sh ./bin/mysql_to_hdfs.sh

```


### 训练电影Embedding
```
python TFRecModel/src/com/sparrowrecsys/offline/tensorflow/HDFSMoviesBERTEmbedding.py

```


### 启动Parameter Servers 和 Workers
```
export TF_CONFIG='{"cluster":{"worker":["localhost:12345","localhost:12346"],"ps":["localhost:23456","localhost:23457"],"chief":["localhost:34567"]},"task":{"type":"worker","index":0}}'

nohup python TFRecModel/src/com/sparrowrecsys/offline/tensorflow/TFServer.py &



export TF_CONFIG='{"cluster":{"worker":["localhost:12345","localhost:12346"],"ps":["localhost:23456","localhost:23457"],"chief":["localhost:34567"]},"task":{"type":"worker","index":1}}'

nohup python TFRecModel/src/com/sparrowrecsys/offline/tensorflow/TFServer.py &



export TF_CONFIG='{"cluster":{"worker":["localhost:12345","localhost:12346"],"ps":["localhost:23456","localhost:23457"],"chief":["localhost:34567"]},"task":{"type":"ps","index":0}}'

nohup python TFRecModel/src/com/sparrowrecsys/offline/tensorflow/TFServer.py &



export TF_CONFIG='{"cluster":{"worker":["localhost:12345","localhost:12346"],"ps":["localhost:23456","localhost:23457"],"chief":["localhost:34567"]},"task":{"type":"ps","index":1}}'

nohup python TFRecModel/src/com/sparrowrecsys/offline/tensorflow/TFServer.py &

```

### 按一定的启动频率设置以下几个定时任务:
```
# movie embedding
./bin/spark-submit --name EmbeddingLSH --master yarn --deploy-mode cluster --class com.sparrowrecsys.offline.spark.embedding.EmbeddingLSH ~/work/recsys/SparrowRecSys/target/SparrowRecSys-1.0-SNAPSHOT-jar-with-dependencies.jar

# feature engineering
./bin/spark-submit --name FeatureEngineering --master yarn --deploy-mode cluster --class com.sparrowrecsys.offline.spark.featureeng.FeatureEngForRecModel ~/work/recsys/SparrowRecSys/target/SparrowRecSys-1.0-SNAPSHOT-jar-with-dependencies.jar

# training
export TF_CONFIG='{"cluster":{"worker":["localhost:12345","localhost:12346"],"ps":["localhost:23456","localhost:23457"],"chief":["localhost:34567"]},"task":{"type":"chief","index":0}}'; python TFRecModel/src/com/sparrowrecsys/offline/tensorflow/WideNDeep.py

```


### 启动Web服务器
```
java -jar target/SparrowRecSys-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 启动Tensorflow Serving
```
docker run -t --rm -p 8501:8501 \

  -v "~/work/recsys/SparrowRecSys/tmp_model/widendeep:/models/sparrow_recsys_widedeep" \

  -e MODEL_NAME=sparrow_recsys_widedeep \

  tensorflow/serving &

```



### 启动以下几个实时流数据处理任务
```
python TFRecModel/src/com/sparrowrecsys/nearline/tensorflow/KafkaMoviesBERTEmbedding.py

./bin/flink run -p 2 -c com.sparrowrecsys.nearline.flink.NewMovieHandler  ~/work/recsys/SparrowRecSys/target/SparrowRecSys-1.0-SNAPSHOT-jar-with-dependencies.jar

./bin/flink run -p 2 -c com.sparrowrecsys.nearline.flink.NewRatingHandler  ~/work/recsys/SparrowRecSys/target/SparrowRecSys-1.0-SNAPSHOT-jar-with-dependencies.jar

```

## 清理步骤
```
# 停止Web服务器、Tensorflow Serving

# 停止定时任务和实时流任务.

# 删除MySQL中sparrow_recsys数据库

# 删除redis中sparrow_recsys开头的数据

# 删除 hdfs:///sparrow_recsys/*

# 删除 ./kafka-movie-embeddings.csv ./tmp_model ./tmp_sampledata

# 删除Kafka 日志数据，一般位于/tmp/kafka-logs

```


## 项目数据
项目数据来源于开源电影数据集[MovieLens](https://grouplens.org/datasets/movielens/)，项目自带数据集对MovieLens数据集进行了精简，仅保留1000部电影和相关评论、用户数据。全量数据集请到MovieLens官方网站进行下载，推荐使用MovieLens 20M Dataset。

## SparrowRecSys技术架构
SparrowRecSys技术架构遵循经典的工业级深度学习推荐系统架构，包括了离线数据处理、模型训练、近线的流处理、线上模型服务、前端推荐结果显示等多个模块。以下是SparrowRecSys的架构图：
![alt text](https://github.com/wzhe06/SparrowRecSys/raw/master/docs/sparrowrecsysarch.png)

## SparrowRecSys实现的深度学习模型
* Word2vec (Item2vec)
* DeepWalk (Random Walk based Graph Embedding)
* Embedding MLP
* Wide&Deep
* Nerual CF
* Two Towers
* DeepFM
* DIN(Deep Interest Network)

## 相关论文
* [[FFM] Field-aware Factorization Machines for CTR Prediction (Criteo 2016)](https://github.com/wzhe06/Ad-papers/blob/master/Classic%20CTR%20Prediction/%5BFFM%5D%20Field-aware%20Factorization%20Machines%20for%20CTR%20Prediction%20%28Criteo%202016%29.pdf) <br />
* [[GBDT+LR] Practical Lessons from Predicting Clicks on Ads at Facebook (Facebook 2014)](https://github.com/wzhe06/Ad-papers/blob/master/Classic%20CTR%20Prediction/%5BGBDT%2BLR%5D%20Practical%20Lessons%20from%20Predicting%20Clicks%20on%20Ads%20at%20Facebook%20%28Facebook%202014%29.pdf) <br />
* [[PS-PLM] Learning Piece-wise Linear Models from Large Scale Data for Ad Click Prediction (Alibaba 2017)](https://github.com/wzhe06/Ad-papers/blob/master/Classic%20CTR%20Prediction/%5BPS-PLM%5D%20Learning%20Piece-wise%20Linear%20Models%20from%20Large%20Scale%20Data%20for%20Ad%20Click%20Prediction%20%28Alibaba%202017%29.pdf) <br />
* [[FM] Fast Context-aware Recommendations with Factorization Machines (UKON 2011)](https://github.com/wzhe06/Ad-papers/blob/master/Classic%20CTR%20Prediction/%5BFM%5D%20Fast%20Context-aware%20Recommendations%20with%20Factorization%20Machines%20%28UKON%202011%29.pdf) <br />
* [[DCN] Deep & Cross Network for Ad Click Predictions (Stanford 2017)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BDCN%5D%20Deep%20%26%20Cross%20Network%20for%20Ad%20Click%20Predictions%20%28Stanford%202017%29.pdf) <br />
* [[Deep Crossing] Deep Crossing - Web-Scale Modeling without Manually Crafted Combinatorial Features (Microsoft 2016)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BDeep%20Crossing%5D%20Deep%20Crossing%20-%20Web-Scale%20Modeling%20without%20Manually%20Crafted%20Combinatorial%20Features%20%28Microsoft%202016%29.pdf) <br />
* [[PNN] Product-based Neural Networks for User Response Prediction (SJTU 2016)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BPNN%5D%20Product-based%20Neural%20Networks%20for%20User%20Response%20Prediction%20%28SJTU%202016%29.pdf) <br />
* [[DIN] Deep Interest Network for Click-Through Rate Prediction (Alibaba 2018)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BDIN%5D%20Deep%20Interest%20Network%20for%20Click-Through%20Rate%20Prediction%20%28Alibaba%202018%29.pdf) <br />
* [[ESMM] Entire Space Multi-Task Model - An Effective Approach for Estimating Post-Click Conversion Rate (Alibaba 2018)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BESMM%5D%20Entire%20Space%20Multi-Task%20Model%20-%20An%20Effective%20Approach%20for%20Estimating%20Post-Click%20Conversion%20Rate%20%28Alibaba%202018%29.pdf) <br />
* [[Wide & Deep] Wide & Deep Learning for Recommender Systems (Google 2016)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BWide%20%26%20Deep%5D%20Wide%20%26%20Deep%20Learning%20for%20Recommender%20Systems%20%28Google%202016%29.pdf) <br />
* [[xDeepFM] xDeepFM - Combining Explicit and Implicit Feature Interactions for Recommender Systems (USTC 2018)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BxDeepFM%5D%20xDeepFM%20-%20Combining%20Explicit%20and%20Implicit%20Feature%20Interactions%20for%20Recommender%20Systems%20%28USTC%202018%29.pdf) <br />
* [[Image CTR] Image Matters - Visually modeling user behaviors using Advanced Model Server (Alibaba 2018)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BImage%20CTR%5D%20Image%20Matters%20-%20Visually%20modeling%20user%20behaviors%20using%20Advanced%20Model%20Server%20%28Alibaba%202018%29.pdf) <br />
* [[AFM] Attentional Factorization Machines - Learning the Weight of Feature Interactions via Attention Networks (ZJU 2017)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BAFM%5D%20Attentional%20Factorization%20Machines%20-%20Learning%20the%20Weight%20of%20Feature%20Interactions%20via%20Attention%20Networks%20%28ZJU%202017%29.pdf) <br />
* [[DIEN] Deep Interest Evolution Network for Click-Through Rate Prediction (Alibaba 2019)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BDIEN%5D%20Deep%20Interest%20Evolution%20Network%20for%20Click-Through%20Rate%20Prediction%20%28Alibaba%202019%29.pdf) <br />
* [[DSSM] Learning Deep Structured Semantic Models for Web Search using Clickthrough Data (UIUC 2013)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BDSSM%5D%20Learning%20Deep%20Structured%20Semantic%20Models%20for%20Web%20Search%20using%20Clickthrough%20Data%20%28UIUC%202013%29.pdf) <br />
* [[FNN] Deep Learning over Multi-field Categorical Data (UCL 2016)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BFNN%5D%20Deep%20Learning%20over%20Multi-field%20Categorical%20Data%20%28UCL%202016%29.pdf) <br />
* [[DeepFM] A Factorization-Machine based Neural Network for CTR Prediction (HIT-Huawei 2017)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BDeepFM%5D%20A%20Factorization-Machine%20based%20Neural%20Network%20for%20CTR%20Prediction%20%28HIT-Huawei%202017%29.pdf) <br />
* [[NFM] Neural Factorization Machines for Sparse Predictive Analytics (NUS 2017)](https://github.com/wzhe06/Ad-papers/blob/master/Deep%20Learning%20CTR%20Prediction/%5BNFM%5D%20Neural%20Factorization%20Machines%20for%20Sparse%20Predictive%20Analytics%20%28NUS%202017%29.pdf) <br />

## 其他相关资源
* [Papers on Computational Advertising](https://github.com/wzhe06/Ad-papers) <br />
* [Papers on Recommender System](https://github.com/wzhe06/Ad-papers) <br />
* [CTR Model Based on Spark](https://github.com/wzhe06/SparkCTR) <br />
