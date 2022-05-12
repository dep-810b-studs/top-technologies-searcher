package org.example;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Array;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.example.FileLoading.loadFile;

public class Clustering {
    static Pattern tagPattern = Pattern.compile("<(.+?)>");

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("Classification")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        String inputFile = args[0];
        String outputFile = args[1];

        JavaRDD<Map<String, String>> questions = loadFile(context, inputFile, 16);
        JavaRDD<List<String>> tags = questions
                .map(Clustering::extractTags);
        List<String> distinctTags = tags
                .flatMap(List::iterator)
                .distinct()
                .collect();
        Map<String, Long> tagMap = new HashMap<>();
        Map<Long, String> reverseTagMap = new HashMap<>();
        for(String tag: distinctTags){
            reverseTagMap.put((long)tagMap.size(), tag);
            tagMap.put(tag, (long)tagMap.size());
        }

        Map<Tuple2<Long, Long>, Long> distances = tags
                .map(thisTags -> encodeTags(thisTags, tagMap))
                .flatMap(Clustering::extractAllPairs)
                .countByValue();

        List<Tuple3<Long, Long, Double>> similaritiesList = new ArrayList<>();
        for(Map.Entry<Tuple2<Long, Long>, Long> tagPairCounts: distances.entrySet()){
            similaritiesList.add(new Tuple3<>(tagPairCounts.getKey()._1(),tagPairCounts.getKey()._2(),
                                 Math.pow(tagPairCounts.getValue(), 10)));
        }
        JavaRDD<Tuple3<Long, Long, Double>> similarities = context.parallelize(similaritiesList);

        PowerIterationClustering clustering = new PowerIterationClustering()
                .setK(20)
                .setMaxIterations(60);

        PowerIterationClusteringModel model = clustering.run(similarities);
        JavaRDD<Tuple3<Integer, Long, String>> assignments = model.assignments().toJavaRDD()
                .map(assignment -> new Tuple3<>(assignment.cluster(), assignment.id(), reverseTagMap.get(assignment.id())));

        assignments.mapToPair(assignment -> new Tuple2<>(assignment._1(), assignment._3()))
                .groupByKey()
                .repartition(1)
                .saveAsTextFile(outputFile);

        /*
        Map<Integer, List<String>> mapping = new HashMap<>();
        for(PowerIterationClustering.Assignment assignment: model.assignments().toJavaRDD().collect()){
            mapping.getOrDefault(assignment.cluster(), new ArrayList<>()).add(reverseTagMap.get(assignment.id()));
        }
        for(Map.Entry<Integer, List<String>> entry: mapping.entrySet()){
            for(String tag: entry.getValue()) {
                System.out.print(tag + " ");
            }
            System.out.println();
        }
        */
    }

    private static List<Long> encodeTags(List<String> tags, Map<String, Long> tagMap) {
        List<Long> encoded = new ArrayList<>();
        for(String tag: tags){
            encoded.add(tagMap.get(tag));
        }
        return encoded;
    }

    private static Iterator<Tuple2<Long, Long>> extractAllPairs(List<Long> thisTags) {
        List<Tuple2<Long, Long>> tagPairs = new ArrayList<>();
        for(int i = 0; i < thisTags.size(); ++i){
            for(int j = 0; j < i; ++j){
                Tuple2<Long, Long> pair = new Tuple2<>(thisTags.get(i), thisTags.get(j));
                if(pair._2() < pair._1()){
                    pair = pair.swap();
                }
                tagPairs.add(pair);
            }
        }
        return tagPairs.iterator();
    }

    static List<String> extractTags(Map<String, String> attributes) {
        Matcher tagMatcher = tagPattern.matcher(attributes.get("Tags"));
        List<String> tags = new ArrayList<>();
        while(tagMatcher.find()){
            tags.add(tagMatcher.group(1));
        }
        return tags;
    }
}
