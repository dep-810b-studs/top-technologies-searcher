package ru.mai.dep806.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Пример Word Count на Spark / Java 8.
 */
public class SparkDemo {

    public static void main(String[] args) {

        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf().setAppName("Spark Demo");
        sparkConf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> users = sc.parallelize(Arrays.asList("user1", "user4", "user2", "user3", "user4", "John Smith", "Steve Brown"));

        // Фильтрация
        List<String> result =
                users.filter(user -> user.endsWith("3"))
                        .collect();
        System.out.println(result);

        // Пребразование каждого элемента
        result = users.map(user -> user.toUpperCase())
                .collect();
        System.out.println(result);

        // Преобразование каждого элемента в список элементов
        result = users.flatMap(user -> Arrays.asList(user.split(" ")).iterator())
                .collect();
        System.out.println(result);

        // Сортировка
        result = users.sortBy(user -> user.charAt(user.length() - 1), false, users.getNumPartitions())
                .collect();
        System.out.println(result);

        // Удаление дубликатов
        result = users.distinct()
                .collect();
        System.out.println(result);

        // Группировка
        // [(10,[John Smith]), (11,[Steve Brown]), (5,[user1, user4, user2, user3, user4])]
        List<Tuple2<Integer, Iterable<String>>> groupResult = users.groupBy(user -> user.length())
                .collect();
        System.out.println(groupResult);

        JavaRDD<String> fruits = sc.parallelize(Arrays.asList("Apple", "Orange", "Grape", "Mellon"));

        // Объединение (конкатенация) 2 коллекций в одну
        result = users.union(fruits)
                .collect();
        System.out.println(result);

        JavaRDD<UserAddress> addresses = sc.parallelize(Arrays.asList(
                new UserAddress("user1", "Moscow"),
                new UserAddress("user2", "Kiev"),
                new UserAddress("user3", "Minsk"),
                new UserAddress("user4", "London"),
                new UserAddress("user2", "New York")
        ));

        // Объединение (SQL Join) 2 коллекций
        // [
        //  (user1,(UserAddress{userId='user1', address='Moscow'},user1)),
        //  (user2,(UserAddress{userId='user2', address='Kiev'},user2)),
        //  (user2,(UserAddress{userId='user2', address='New York'},user2)),
        //  (user3,(UserAddress{userId='user3', address='Minsk'},user3)),
        //  (user4,(UserAddress{userId='user4', address='London'},user4)),
        //  (user4,(UserAddress{userId='user4', address='London'},user4))
        // ]
        JavaPairRDD<String, String> userPairs = users
                .mapToPair(user -> new Tuple2(user, user));

        JavaPairRDD<String, UserAddress> addressPair = addresses
                .mapToPair(address -> new Tuple2<>(address.getUserId(), address));

        List<Tuple2<String, Tuple2<UserAddress, String>>> joined =
                addressPair.join(userPairs)
                        .collect();
        System.out.println(joined);


        // Соединение нескольких коллекций в одну по ключу
        // [
        //  (John Smith,([],[John Smith])),
        //  (user3,([UserAddress{userId='user3', address='Minsk'}],[user3])),
        //  (user1,([UserAddress{userId='user1', address='Moscow'}],[user1])),
        //  (user2,([UserAddress{userId='user2', address='Kiev'}, UserAddress{userId='user2', address='New York'}],[user2])),
        //  (Steve Brown,([],[Steve Brown])),
        //  (user4,([UserAddress{userId='user4', address='London'}],[user4, user4]))
        // ]
        List<Tuple2<String, Tuple2<Iterable<UserAddress>, Iterable<String>>>> cogrouped =
                addressPair.cogroup(userPairs)
                        .collect();
        System.out.println(cogrouped);

        // Подсчет количества элементов в коллекции
        System.out.println(users.count());

        // Поиск минимального/максимального элементов
        System.out.println(users.max(new LengthComparator()));
        System.out.println(users.min(new LengthComparator()));

        // Первые 2 элемента в RDD
        System.out.println(users.take(2));
        
        // Первые 2 отсортированные компаратором по-умолчанию
        System.out.println(users.top(2));

        // Распространение объекта cities по узлам для более оптимального использования внутри трансформаций
        List<String> cities = Arrays.asList("Kiev", "Moscow");
        Broadcast<List<String>> broadcastedCities = sc.broadcast(cities);

        List<UserAddress> filteredAddresses = addresses.filter(address -> broadcastedCities.getValue().contains(address.getAddress()))
                .collect();
        System.out.println(filteredAddresses);
    }

    public static class LengthComparator implements Serializable, Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return o1.length() - o2.length();
        }
    }

    public static class UserAddress implements Serializable {
        private String userId;
        private String address;

        public UserAddress(String userId, String address) {
            this.userId = userId;
            this.address = address;
        }

        public String getUserId() {
            return userId;
        }

        public String getAddress() {
            return address;
        }

        @Override
        public String toString() {
            return "UserAddress{" +
                    "userId='" + userId + '\'' +
                    ", address='" + address + '\'' +
                    '}';
        }
    }


}
