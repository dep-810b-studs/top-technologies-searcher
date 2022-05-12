package ru.mai.dep806.bigdata.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 spark-submit --class ru.mai.dep806.bigdata.spark.TopHadoopUsers
 --master yarn
 --deploy-mode cluster
 --driver-memory 100M
 --executor-memory 500M
 spark-jobs-1.0-SNAPSHOT.jar
 /user/stud/stackoverflow/landing/posts_sample
 /user/stud/stackoverflow/landing/users
 /user/stud/eugene/top_hadoop_users
 */
public class TopHadoopUsers {
    public static void main(String[] args) {

        String postsPath = args[0];
        String usersPath = args[1];
        String outPath = args[2];

        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf().setAppName("Spark Tags Popularity");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Создаем RDD для пользователей
        JavaPairRDD<String, String> usersById = sc.textFile(usersPath)
                // Парсим из XML в Map
                .map(line -> XmlUtils.parseXmlToMap(line))
                // Конвертируем в пары <Id, Имя>
                .mapToPair(user -> new Tuple2<>(user.get("Id"), user.get("DisplayName")));

        // Чтение постов из файла
        sc.textFile(postsPath)
                // Парсим XML в Map
                .map(XmlUtils::parseXmlToMap)
                // Отфильтроваем посты с пустыми полями (чтобы было меньше данных и потом не думать об этом)
                .filter(post -> StringUtils.isNotBlank(post.get("OwnerUserId")) && StringUtils.isNotBlank(post.get("Tags")))
                // Конвертируем в пары <OwnerUserId, список тегов поста>
                .mapToPair(post ->
                        new Tuple2<>(
                                post.get("OwnerUserId"),
                                XmlUtils.splitTags(post.get("Tags"))
                        )
                )
                // Оставляем только те посты, у которых есть тег hadoop
                .filter(post -> post._2.contains("hadoop"))
                // Аггрегируем по ключу (OwnerUserId, список тегов) =>(OwnerUserId, кол-во постов)
                .aggregateByKey(
                        0,  // Начальное значение счетчика
                        (count, tags) -> count + 1, // Инкремент счетчика
                        (count1, count2) -> count1 + count2 // функция слияния двух счетчиков
                )
                .filter(pair -> pair._2 > 1)
                // Джойним (inner join) с пользователями по ключу OwnerUserId = Id
                .join(usersById)
                // В результате каждая строка - комбинация пар <OwnerUserId, <Счетчик, UserName>>
                // Конвертируем в пару <Счетчик, UserName>
                .mapToPair(pair -> new Tuple2<>(pair._2()._1(), pair._2()._2()))
                // Сортируем по ключу (Счетчик) по убыванию
                .sortByKey(false)
                // Конвертируем пару в строку "UserName - счетчик кол-ва постов"
                .map(pair -> pair._2() + " - " + pair._1())
                .coalesce(1)
                // Сохраняем на HDFS
                .saveAsTextFile(outPath);
    }
}
