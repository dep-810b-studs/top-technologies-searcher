package ru.mai.dep806.bigdata.spark;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.to_timestamp;

public class SparkTimestampTest {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Spark save to Database");
        sparkConf.setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String, String> value = new HashMap<>();
        value.put("CreationDate", "2008-07-31T14:22:31.287");

        sqlContext.createDataFrame(
                ImmutableList.of(new GenericRow(new Object[]{value})),
                new StructType( new StructField[] {
                        DataTypes.createStructField("c0", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true)
                })
        ).select(to_timestamp(expr("c0.CreationDate"), "yyyy-MM-dd'T'HH:mm:ss.SSS").as("time")
        ).show();
    }

}
