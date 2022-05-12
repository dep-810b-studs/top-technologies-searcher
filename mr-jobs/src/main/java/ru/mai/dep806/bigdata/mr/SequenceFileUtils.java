package ru.mai.dep806.bigdata.mr;

import java.util.Map;

public class SequenceFileUtils {

    private static final char FIELD_SEPARATOR = '\01';

    public static String toSequenceString(Map<String, String> row, String[] fields) {
        final StringBuilder buffer = new StringBuilder();
        for (String field : fields) {
            String fieldValue = row.get(field);
            if (fieldValue != null) {
                buffer.append(fieldValue);
            }
            buffer.append(FIELD_SEPARATOR);
        }
        return buffer.toString();
    }
}
