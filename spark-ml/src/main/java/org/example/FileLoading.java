package org.example;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileLoading {
    static Pattern xmlAttribute = Pattern.compile(" (\\w+)=\"(.+?)\"");

    public static String unescape(String s) {
        return StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeXml(s));
    }

    public static Map<String, String> lineToMap(String line) {
        Matcher matcher = xmlAttribute.matcher(line);
        Map<String, String> attributes = new HashMap<>();
        while (matcher.find()) {
            attributes.put(matcher.group(1), unescape(matcher.group(2)));
        }
        return attributes;

    }

    public static JavaRDD<Map<String, String>> loadFile(JavaSparkContext context, String filename, int minPartitions) {
        return context.textFile(filename, minPartitions)
                .map(line -> lineToMap(line))
                .filter(attributes -> attributes.getOrDefault("PostTypeId", "").equals("1"));
    }

    public static Map<String, Integer> loadTagClusters(JavaSparkContext context, String filename) {
        List<Tuple2<String, Integer>> mappingList = context.textFile(filename)
                .flatMap(line -> {
                    line = line.substring(1, line.length() - 1);
                    String[] split = line.split(",", 2);
                    Integer cluster = Integer.parseInt(split[0]);
                    String[] tags = split[1].substring(1, split[1].length() - 1).split(", ");
                    List<Tuple2<String, Integer>> clusterMap = new ArrayList<>();
                    for(String tag: tags){
                        clusterMap.add(new Tuple2<>(tag, cluster));
                    }
                    return clusterMap.iterator();
                }).collect();
        Map<String, Integer> mapping = new HashMap<>();
        for(Tuple2<String, Integer> map: mappingList){
            mapping.put(map._1(), map._2());
        }
        return mapping;
    }
}