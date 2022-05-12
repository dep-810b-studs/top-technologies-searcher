package ru.mai.dep806.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

/**
 * Пример использования Spark DataFrame и Spark SQL:
 * Берет названия городов из таблицы в Oracle, Пользователей StackOverflow из XML файла на HDFS
 * Выполняет JOIN по названию города/локации пользователя и выводит результаты в файл на HDFS.
 *
 * Запуск:
 spark-submit --class ru.mai.dep806.bigdata.spark.UsersLocation  \
 --master yarn --deploy-mode cluster --driver-memory 500M --executor-memory 500M \
 --jars /var/lib/sqoop/ojdbc7-12.1.0.2.jar  \
 spark-jobs-1.0-SNAPSHOT.jar  \
 /user/stud/stackoverflow/landing/users \
 jdbc:oracle:thin:@172.16.82.250:1521/orcl oltp_test oltp_test \
 /user/stud/eugene/users_with_cities
 */
public class UsersLocation {

    private static final String[] USER_FIELDS = new String[]{
            "Id", "Reputation", "CreationDate", "DisplayName", "LastAccessDate", "Location", "Views",
            "UpVotes", "DownVotes", "Age", "AccountId"
    };

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Users Location");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        String usersPath = args[0];
        String url = args[1];
        String login = args[2];
        String password = args[3];
        String outPath = args[4];

        // Параметры подключения к БД
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", login);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", "oracle.jdbc.OracleDriver");

        // Чтение таблицы CITY и регистрация временной таблицы в текущем SQLContext
        sqlContext.read().jdbc(url, "CITY", connectionProperties).registerTempTable("CITY");

        // Чтение пользователей из XML файла и преобразование в объекты Row
        JavaRDD<Row> userRdd = sparkContext.textFile(usersPath).map(user -> XmlUtils.parseXmlToRow(user, USER_FIELDS));

        // Создание датафрейма и временной таблицы USER из RDD.
        sqlContext.createDataFrame(userRdd, XmlUtils.createDataFrameStruct(USER_FIELDS))
                .registerTempTable("USER");

        // SQL Запрос с использованием временных таблиц USER и CITY (имена колонок и таблиц - case sensitive)
        sqlContext.sql("select Id, DisplayName, Location, COUNTRY, CITY_NAME, CITY_ID" +
                " from USER inner join CITY on (upper(USER.Location) = upper(CITY.CITY_NAME))")
                // Результат ожидается небольшой, поэтому сливаем все партиции в одну
                .coalesce(1)
                // Создаем writer
                .write()
                // Используя формат JSON
                .format("org.apache.spark.sql.json")
                // С перезаписью
                .mode(SaveMode.Overwrite)
                // Сохраняем outPathв файл (терминальное действие - инициирует запуск всей цепочки)
                .save(outPath);
    }
}
