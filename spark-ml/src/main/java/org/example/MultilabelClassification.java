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
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.evaluation.MultilabelMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;
import static org.example.Clustering.extractTags;
import static org.example.FileLoading.loadFile;
import static org.example.FileLoading.loadTagClusters;

public class MultilabelClassification {
    public static class LabeledDocument {
        String text;
        List<Double> labels;

        LabeledDocument(String text, List<Double> labels) {
            this.text = text;
            this.labels = labels;
        }

        public String getText() {
            return text;
        }

        public List<Double> getLabels() {
            return labels;
        }
    }

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("Classification")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        String inputFile = args[0];
        String clusteringFile = args[1];

        Map<String, Integer> tagClustering = loadTagClusters(context, clusteringFile);
        JavaRDD<LabeledDocument> questions = loadFile(context, inputFile, 16)
                .map(attributes -> {
                    List<String> tags = extractTags(attributes);
                    List<Double> encodedTags = new ArrayList<>();
                    for(String tag: tags){
                        encodedTags.add((double)tagClustering.getOrDefault(tag, 20));
                    }
                    return new LabeledDocument(attributes.get("Title") + " " + attributes.get("Body"), encodedTags);
                });
        JavaRDD<LabeledDocument>[] splits = questions.randomSplit(new double[]{0.7, 0.3}, 1);
        JavaRDD<Row> trainRDD = splits[0]
                .flatMap(row -> {
                    List<Row> rows = new ArrayList<>();
                    List<Double> tagIds = row.getLabels();
                    for(Object tagId: tagIds){
                        rows.add(RowFactory.create(row.getText(), tagId));
                    }
                    return rows.iterator();
                });
        JavaRDD<LabeledDocument> testRDD = splits[1];


        StructType trainSchema = new StructType(new StructField[]{
                new StructField("text", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> train = session.createDataFrame(trainRDD, trainSchema);
        Dataset<Row> test = session.createDataFrame(testRDD, LabeledDocument.class);

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
        NaiveBayes classifier = new NaiveBayes();
//        LogisticRegression classifier = new LogisticRegression();
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                tokenizer, stopWordsRemover, countVectorizer, idf, classifier
        });

        PipelineModel model = pipeline.fit(train);
        Dataset<Row> predictions = model.transform(test);

        session.sqlContext().udf()
                .register("prediction", (Double prediction) -> new Double[]{prediction}, DataTypes.createArrayType(DataTypes.DoubleType));
        predictions = predictions
                .withColumn("predictionArray", callUDF("prediction", predictions.col("prediction")));

        MultilabelMetrics metrics = new MultilabelMetrics(predictions.select("labels", "predictionArray"));

        predictions.groupBy("labels", "prediction")
                .count().orderBy("labels", "prediction").show(1000);
        System.out.println("accuracy = " + metrics.accuracy());
    }
}
