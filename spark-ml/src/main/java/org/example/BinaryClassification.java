package org.example;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;

import static org.apache.spark.sql.functions.callUDF;
import static org.example.FileLoading.loadFile;

public class BinaryClassification {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder()
                .appName("Classification")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        String inputFile = args[0];
        String outputFile = args[1];

        JavaRDD<Row> questions = loadFile(context, inputFile, 16)
                .map(attributes -> RowFactory.create(attributes.get("Title") + " " + attributes.get("Body"),
                        attributes.get("Tags").contains("<javascript>") ? 1. : 0.));

        StructType schema = new StructType(new StructField[]{
                new StructField("text", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> questionsDataset = session.createDataFrame(questions, schema);
        Dataset<Row>[] splits = questionsDataset.randomSplit(new double[]{0.7, 0.3}, 1);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        /*
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
                */
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
        /*
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(100000)
                .setInputCol(stopWordsRemover.getOutputCol())
                .setOutputCol("rawFeatures");
                */
        IDF idf = new IDF()
                .setInputCol(countVectorizer.getOutputCol())
                .setOutputCol("features");
//        NaiveBayes classifier = new NaiveBayes();
//        LogisticRegression classifier = new LogisticRegression();
        RandomForestClassifier classifier = new RandomForestClassifier().setSeed(1);
//        GBTClassifier classifier = new GBTClassifier().setSeed(1);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                tokenizer, stopWordsRemover, countVectorizer, idf, classifier
        });
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setMetricName("areaUnderROC")
                .setRawPredictionCol("probability");
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(classifier.numTrees(), new int[]{5, 10, 20, 40})
                .build();
        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(3);

        CrossValidatorModel model = cv.fit(train);
        model.save(outputFile);
//        PipelineModel model = pipeline.fit(train);
        Dataset<Row> predictions = model.transform(test);

        double[] thresholds = getThresholds(session, predictions);
        PipelineModel m = (PipelineModel) model.bestModel();//.stages()[2].set("thresholds", thresholds);
        m.stages()[4].set("thresholds", thresholds);
        predictions = model.transform(test);

        double evaluation = evaluator.evaluate(predictions);

//        outputROC(session, outputFile, predictions);

        predictions.groupBy("label", "prediction")
                .count().show();
        System.out.println(evaluator.getMetricName() + " = " + evaluation);
    }

    private static double[] getThresholds(SparkSession session, Dataset<Row> predictions) {
        session.sqlContext().udf()
                .register("getRawProbability", (Vector prob) -> prob.apply(1), DataTypes.DoubleType);
        predictions = predictions
                .withColumn("rawProbability", callUDF("getRawProbability", predictions.col("probability")));

        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(predictions.select("rawProbability", "label"));

        JavaRDD<Tuple2<Object, Object>> fMeasure = metrics.fMeasureByThreshold().toJavaRDD();
        Tuple2<Object, Object> bestFMeasure = fMeasure.reduce((lhs, rhs) -> {
            if((Double)lhs._2() > (Double)rhs._2()){
                return lhs;
            }
            return rhs;
        });

        double bestThreshold = (Double)bestFMeasure._1();
        return new double[]{1 - bestThreshold, bestThreshold};
    }

    private static void outputROC(SparkSession session, String outputFile, Dataset<Row> predictions) {
        session.sqlContext().udf()
                .register("getRawProbability", (Vector prob) -> prob.apply(1), DataTypes.DoubleType);
        predictions = predictions
                .withColumn("rawProbability", callUDF("getRawProbability", predictions.col("probability")));

        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(predictions.select("rawProbability", "label"));

        metrics.roc().saveAsTextFile(outputFile);
    }
}
