package ru.mai.dep806.bigdata.mr;

import java.util.Arrays;
import java.util.OptionalInt;

/**
 * Простейший пример Map-Reduce на Java 8.
 */
public class JavaMapReduce {

    public static void main(String[] args) {
        int[] data = new int[] {5, 8, 3, 6, 3, 7};

        OptionalInt result = Arrays.stream(data)
                .map(i -> i + 1)
                .reduce( (a, b) -> a + b);

        System.out.println(result);
    }
}
