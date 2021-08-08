# upload data from mysql to hdfs.

mysql -usparrow -pRecsys1$ -Dsparrow_recsys -e "select * from movies" -N  > movies.csv
mysql -usparrow -pRecsys1$ -Dsparrow_recsys -e "select * from ratings" -N  > ratings.csv

movie_count=`cat movies.csv | wc -l`
rating_count=`cat ratings.csv | wc -l`

echo 'movie_count' $movie_count
echo 'rating_count' $rating_count

if  [ "100" -gt "$movie_count" ]; then
    echo "invalid movies data from mysql."
    exit 1
fi

if  [ "100" -gt "$rating_count" ]; then
    echo "invalid ratings data from mysql."
    exit 1
fi

hdfs dfs -rm -r hdfs:///sparrow_recsys/movies/*
hdfs dfs -rm -r hdfs:///sparrow_recsys/ratings/*

hdfs dfs -mkdir -p hdfs:///sparrow_recsys/movies/0000
hdfs dfs -mkdir -p hdfs:///sparrow_recsys/ratings/0000

hdfs dfs -put movies.csv hdfs:///sparrow_recsys/movies/0000/part-0
hdfs dfs -put ratings.csv hdfs:///sparrow_recsys/ratings/0000/part-0

rm movies.csv ratings.csv
