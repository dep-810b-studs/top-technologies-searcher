package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Int;

import java.util.Map;

import static org.example.Clustering.extractTags;
import static org.example.FileLoading.loadFile;
import static org.example.FileLoading.loadTagClusters;

public class MulticlassClassification {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("Classification")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        String inputFile = args[0];
        String clusteringFile = args[1];

        Map<String, Integer> tagClustering = loadTagClusters(context, clusteringFile);
        JavaRDD<Row> questions = loadFile(context, inputFile, 16)
                .map(attributes -> RowFactory.create(attributes.get("Title") + " " + attributes.get("Body"),
                        (double)tagClustering.getOrDefault(extractTags(attributes).get(0), 20)));

        StructType schema = new StructType(new StructField[]{
                new StructField("text", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> questionsDataset = session.createDataFrame(questions, schema);
        Dataset<Row>[] splits = questionsDataset.randomSplit(new double[]{0.7, 0.3}, 1);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        RegexTokenizer tokenizer = new RegexTokenizer()
                .setPattern("[-'_+#a-zA-Z0-9]+")
                .setGaps(false)
                .setToLowercase(true)
                .setInputCol("text")
                .setOutputCol("words");
        StopWordsRemover stopWordsRemover = new StopWordsRemover()
                .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("clearWords");
        CountVectorizer countVectorizer = new CountVectorizer()
                .setMinDF(100)
                .setVocabSize(100000)
                .setInputCol(stopWordsRemover.getOutputCol())
                .setOutputCol("rawFeatures");
        IDF idf = new IDF()
                .setInputCol(countVectorizer.getOutputCol())
                .setOutputCol("features");
//        NaiveBayes classifier = new NaiveBayes();
        LogisticRegression classifier = new LogisticRegression();
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                tokenizer, stopWordsRemover, countVectorizer, idf, classifier
        });

        PipelineModel model = pipeline.fit(train);
        Dataset<Row> predictions = model.transform(test);

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");

        double accuracy = evaluator.evaluate(predictions);

        predictions.groupBy("label", "prediction")
                .count().orderBy("label", "prediction").show(1000);
        System.out.println("accuracy = " + accuracy);
    }
}
