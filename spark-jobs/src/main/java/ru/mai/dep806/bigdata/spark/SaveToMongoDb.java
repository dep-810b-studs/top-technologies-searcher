package ru.mai.dep806.bigdata.spark;

import com.mongodb.spark.MongoSpark;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

/**
 * Сохранение Stackoverflow данных из XML в ElasticSearch.
 * <p>
 * Локальный запуск из Idea:
 * Правый клик -> Create "SaveToMongoDb.main()"
 * Установить Program Arguments (пример):
 * C:\tmp\ru.stackoverflow.com\Users.xml
 * mongodb://localhost/test.Users
 */
public class SaveToMongoDb {

    public static void main(String[] args) {

        String path = args[0];
        String mongoDbUri = args[1];

        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf().setAppName("Spark Users Load to MongoDB");
        // включить для локального запуска:
        sparkConf.setMaster("local[4]");
        sparkConf.set("spark.mongodb.output.uri", mongoDbUri);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // чтение данных из XML, конвертация в хэш-таблицу и замена HTML-entity на обычные символы
        JavaRDD<Document> rdd = sc.textFile(path)
                .map(XmlUtils::parseXmlToMap)
                .filter(m -> StringUtils.isNotBlank(m.get("Id")))
                .map(XmlUtils::unescape)
                .map(d -> new Document(Collections.unmodifiableMap(d)));

        // сохранение в MongoDB
        MongoSpark.save(rdd);
    }

}
