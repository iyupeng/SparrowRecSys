-- sparrow data tables in hive.
-- NOTE: some configurations are required to access child directories recursively.
-- For example in spark-sql:
--      SET hive.mapred.supports.subdirectories=true;
--      SET mapreduce.input.fileinputformat.input.dir.recursive=true;

create database sparrow_recsys;
use sparrow_recsys;

CREATE EXTERNAL TABLE IF NOT EXISTS movies
(movieId bigint, title string, genres string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs:///sparrow_recsys/movies/';


CREATE EXTERNAL TABLE IF NOT EXISTS ratings
(userId bigint, movieId bigint, rating float, `time` bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs:///sparrow_recsys/ratings/';
