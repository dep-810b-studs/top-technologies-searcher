package ru.mai.dep806.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Пример Word Count на Spark / Java 8.
 *
 * Запуск на Hadoop:

 spark-submit --class ru.mai.dep806.bigdata.spark.WordCount \
 --master local \
 --deploy-mode cluster \
 --driver-memory 100M \
 --executor-memory 500M \
 spark-jobs-1.0-SNAPSHOT.jar \
 /user/stud/eugene/library \
 /user/stud/eugene/spark-wc


 DAG:

 (3) ShuffledRDD[4] at reduceByKey at WordCount.java:45 []
 +-(3) MapPartitionsRDD[3] at mapToPair at WordCount.java:43 []
 |  MapPartitionsRDD[2] at flatMap at WordCount.java:37 []
 |  /user/stud/textdata MapPartitionsRDD[1] at textFile at WordCount.java:35 []
 |  /user/stud/textdata HadoopRDD[0] at textFile at WordCount.java:35 []

 */
public class WordCount {

    public static void main(String[] args) {

        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf().setAppName("Spark Word Count");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Чтение файлов из директории, указанной первым аргументом командной строки
        JavaRDD<String> words = sc.textFile(args[0])
                // Для каждой строки: разделить ее на слова по пробелам и слить в общий список
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // В итоге words содержит список всех слов

        JavaPairRDD<String, Integer> counts =
                // Каждое слово преобразуем в пару (слово, 1). 1 - обозначает, что слово встретилось 1 раз.
                words.mapToPair(word -> new Tuple2<>(word, 1))
                        // Выполняем группировку по ключу (слову), суммируя значения
                        .reduceByKey((x, y) -> x + y)
                .persist(StorageLevel.MEMORY_AND_DISK());

        // В итоге counts содержит пары (слово, кол-во).

        // Печатаем на консоль драйвера план выполнения RDD
        System.out.println(counts.toDebugString());

        // В этот момент код еще не выполнился,
        // реальное выполнение произойдет когда выполнится терминальная операция,
        // например, сохранение результата в место (указано вторым аргументом командной строки)

        List<String> top10Words = counts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .repartition(1)
                .map(pair -> pair._1() + " " + pair._2())
                .take(10);

        System.out.println(top10Words);

        List<String> bottom10Words = counts
                .mapToPair(Tuple2::swap)
                .sortByKey(true)
                .repartition(1)
                .map(pair -> pair._1() + " " + pair._2())
                .take(10);

        System.out.println(bottom10Words);
    }
}
