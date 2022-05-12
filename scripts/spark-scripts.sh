 spark-submit --class ru.mai.dep806.bigdata.spark.SaveToElasticSearch \
 --master yarn     --deploy-mode cluster     --driver-memory 100M \
 --executor-memory 500M     spark-jobs-1.0-SNAPSHOT.jar    \
 /user/stud/stackoverflow/landing/posts_sample \
 172.16.82.120:9200,172.16.82.121:9200,172.16.82.122:9200,172.16.82.123:9200 \
 stackoverflow/data-posts-sample