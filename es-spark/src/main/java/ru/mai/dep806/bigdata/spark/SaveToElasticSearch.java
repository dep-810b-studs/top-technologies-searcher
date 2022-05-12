package ru.mai.dep806.bigdata.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Collections;
import java.util.Map;

/**
 * Сохранение Stackoverflow данных из XML в ElasticSearch.

 Запуск на hadoop yarn:
    spark-submit --class ru.mai.dep806.bigdata.spark.SaveToElasticSearch \
    --master yarn     --deploy-mode cluster     --driver-memory 100M \
    --executor-memory 500M     spark-jobs-1.0-SNAPSHOT.jar    \
    /user/stud/stackoverflow/landing_subset/posts \
    172.16.82.120:9300,172.16.82.121:9300,172.16.82.122:9300,172.16.82.123:9300, \
    stackoverflow/data-posts-sample

 Локальный запуск из Idea:
 Правый клик -> Create "SaveToElasticSearch.main()"
 Установить Program Arguments (пример):
    C:\tmp\ru.stackoverflow.com\Users.xml
    172.16.82.120:9200,172.16.82.121:9200,172.16.82.122:9200,172.16.82.123:9200
    ru.stackoverflow/user
 */
public class SaveToElasticSearch {

    public static void main(String[] args) {

        String path = args[0];
        String elasticNodes = args[1];
        String elasticResource = args[2];

        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf().setAppName("Spark Tags Popularity");
        // включить для локального запуска:
        sparkConf.setMaster("local[4]");
        sparkConf.set("es.nodes", elasticNodes);
        sparkConf.set("es.http.timeout", "5m");
        sparkConf.set("es.http.retries", "10");
        sparkConf.set("es.batch.write.retry.count", "10");
        sparkConf.set("es.batch.write.retry.wait", "30s");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // чтение данных из XML, конвертация в хэш-таблицу и замена HTML-entity на обычные символы
        JavaRDD<Map<String, String>> rdd = sc.textFile(path)
                .map(XmlUtils::parseXmlToMap)
                .filter(m -> StringUtils.isNotBlank(m.get("Id")))
                .map(XmlUtils::unescape);

        // сохранение в Elastic Search, используя Id как уникальных идентификатор документа
        JavaEsSpark.saveToEs(rdd, elasticResource, Collections.singletonMap("es.mapping.id", "Id"));
    }

}
