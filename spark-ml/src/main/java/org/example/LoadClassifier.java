package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

import static org.example.FileLoading.loadFile;

public class LoadClassifier {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder()
                .appName("Classification")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        String inputFile = args[0];
        String modelFile = args[1];

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

        CrossValidatorModel model = CrossValidatorModel.load(modelFile);
        Dataset<Row> predictions = model.transform(test);

//        double[] thresholds = getThresholds(session, predictions);
//        PipelineModel m = (PipelineModel) model.bestModel();//.stages()[2].set("thresholds", thresholds);
//        m.stages()[4].set("thresholds", thresholds);
//        predictions = model.transform(test);

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setMetricName("areaUnderROC")
                .setRawPredictionCol("probability");
        double evaluation = evaluator.evaluate(predictions);

//        outputROC(session, outputFile, predictions);

        predictions.groupBy("label", "prediction")
                .count().show();
        System.out.println(evaluator.getMetricName() + " = " + evaluation);
    }

}
