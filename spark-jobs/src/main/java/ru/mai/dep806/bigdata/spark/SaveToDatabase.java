package ru.mai.dep806.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.to_timestamp;
import static ru.mai.dep806.bigdata.spark.XmlUtils.parseXmlToMap;
import static ru.mai.dep806.bigdata.spark.XmlUtils.unescape;

/**
 * Сохранение Stackoverflow данных из XML в базу данных.
 * <p>
 * Запуск на hadoop yarn:
 * spark-submit --class ru.mai.dep806.bigdata.spark.SaveToDatabase \
 * --master yarn     --deploy-mode cluster     --driver-memory 100M \
 * --executor-memory 500M     spark-jobs-1.0-SNAPSHOT.jar    \
 * /user/stud/stackoverflow/landing_subset/posts \
 * 172.16.82.120:9300,172.16.82.121:9300,172.16.82.122:9300,172.16.82.123:9300, \
 * stackoverflow/data-posts-sample
 * <p>
 * Локальный запуск из Idea:
 * Правый клик -> Create "SaveToDatabase.main()"
 * Установить Program Arguments (пример):
 * C:\tmp\ru.stackoverflow.com\Users.xml
 * 172.16.82.120:9200,172.16.82.121:9200,172.16.82.122:9200,172.16.82.123:9200
 * ru.stackoverflow/user
 */
public class SaveToDatabase {

    static String basePath = "C:/Dep806/data/ru/"; // args[0];
    static String dbUrl = "jdbc:oracle:thin:@localhost:1521:orcl";
    static String dbLogin = "so_ru"; // args[2];
    static String dbPassword = "so_ru"; // args[3];

    static Function<String, Column> booleanExpression = field -> expr("if(" + field + " == 'True', 1, 0)");
    static Function<String, Column> dateExpression1 = field -> to_timestamp(expr(field), "yyyy-MM-dd'T'HH:mm:ss.SSS");
    static Function<String, Column> dateExpression2 = field -> to_timestamp(expr(field), "yyyy-MM-dd HH:mm:ss");
    static Function<String, Column> intExpression = field -> expr(field).cast(DataTypes.IntegerType);
    static Function<String, Column> defaultExpression = field -> expr(field);

    public static void main(String[] args) {

        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf().setAppName("Spark save to Database");
        // включить для локального запуска:
        sparkConf.setMaster("local[12]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        UDF1<String, Map<String, String>> parse = (String xml) -> unescape(parseXmlToMap(xml));

        sqlContext.udf().register("parse", parse,
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

//        parseXmlFileToDatabase(sqlContext, basePath + "Users.xml", "USERS",
//                "Id,Reputation,CreationDate,DisplayName,LastAccessDate,WebsiteUrl,Location,AboutMe,Views,UpVotes,DownVotes,ProfileImageUrl,EmailHash,AccountId",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "Badges.xml", "BADGES",
//                "Id,UserId,Name,Date,Class,TagBased",
//                (String field) -> field.endsWith("TagBased") ? booleanExpression.apply(field) : null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "PostLinks.xml","POSTLINKS",
//                "Id,CreationDate,PostId,RelatedPostId,LinkTypeId",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "Tags.xml","TAGS",
//                "Id,TagName,Count,ExcerptPostId,WikiPostId",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "Votes.xml","VOTES",
//                "Id,PostId,VoteTypeId,UserId,CreationDate,BountyAmount",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "Posts.xml","POSTS",
//                "Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,DeletionDate,Score,ViewCount,Body,OwnerUserId,OwnerDisplayName,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,ClosedDate,CommunityOwnedDate",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "PostHistory.xml", "POSTHISTORY",
//                "Id,PostHistoryTypeId,PostId,RevisionGUID,CreationDate,UserId,UserDisplayName,Comment,Text",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "Comments.xml", "COMMENTS",
//                "Id,PostId,Score,Text,CreationDate,UserDisplayName,UserId",
//                null
//        );
//        parseXmlFileToDatabase(sqlContext, basePath + "Posts.xml","POSTS",
//                "Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,DeletionDate,Score,ViewCount,OwnerUserId,OwnerDisplayName,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,ClosedDate,CommunityOwnedDate",
//                null
//        );
//
//        parseCsvFileToDatabase(sqlContext, basePath + "CloseAsOffTopicReasonTypes.csv", "CloseAsOffTopicReasonTypes",
//                (String field) -> field.endsWith("IsUniversal") ? booleanExpression.apply(field) : null);
//        parseCsvFileToDatabase(sqlContext, basePath + "CloseReasonTypes.csv", "CloseReasonTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "FlagTypes.csv", "FlagTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PendingFlags.csv", "PendingFlags", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PostFeedback.csv", "PostFeedback", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PostHistoryTypes.csv", "PostHistoryTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PostNotices.csv", "PostNotices", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PostNoticeTypes.csv", "PostNoticeTypes",
//                (String field) -> field.endsWith("IsHidden") || field.endsWith("Predefined") ? booleanExpression.apply(field) : null
//                );
//        parseCsvFileToDatabase(sqlContext, basePath + "PostTags.csv", "PostTags", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PostTypes.csv", "PostTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "ReviewRejectionReasons.csv", "ReviewRejectionReasons", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "ReviewTaskResults.csv", "ReviewTaskResults", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "ReviewTaskResultTypes.csv", "ReviewTaskResultTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "ReviewTasks.csv", "ReviewTasks", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "ReviewTaskStates.csv", "ReviewTaskStates", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "ReviewTaskTypes.csv", "ReviewTaskTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "SuggestedEdits.csv", "SuggestedEdits", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "SuggestedEditVotes.csv", "SuggestedEditVotes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "TagSynonyms.csv", "TagSynonyms", null);
        parseCsvFileToDatabase(sqlContext, basePath + "VoteTypes.csv", "VoteTypes", null);
//        parseCsvFileToDatabase(sqlContext, basePath + "PostsWithDeleted.csv", "PostsWithDeleted", null);

    }

    private static void parseXmlFileToDatabase(
            final SQLContext sqlContext,
            final String path,
            final String table,
            final String fields,
            final Function<String, Column> customColumnExpressions
    ) {
        System.out.println("Processing " + path);
        Dataset<Row> df = sqlContext.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("delimiter", new String(new char[]{0}))
                .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
                .option("quote", null)
                .load(path);

        List<Function<String, Column>> columnExpressions =
                Stream.of(
                        customColumnExpressions,
                        field -> field.endsWith("Date") ? dateExpression1.apply(field) : null,
                        defaultExpression
                )
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        List<Column> columns = Arrays.stream(fields.split(","))
                .map(field ->
                        columnExpressions
                                .stream()
                                .map(it -> it.apply("map." + field))
                                .filter(Objects::nonNull)
                                .findFirst()
                                .orElseThrow(() -> new IllegalArgumentException("Converter not found for field " + field))
                                .as(field)
                ).collect(Collectors.toList());

        Dataset<Row> result = df.select(callUDF("parse", col("_c0")).as("map"))
                .filter("map.Id is not null")
                .select(JavaConverters.asScalaBufferConverter(columns).asScala().toSeq())
                .as(table);

        result.write()
                .mode(SaveMode.Overwrite)
                .option("user", dbLogin)
                .option("password", dbPassword)
                .option("numPartition", "8")
                .option("truncate", "true")
                .jdbc(dbUrl, table, new Properties());
    }

    private static void parseCsvFileToDatabase(
            final SQLContext sqlContext,
            final String path,
            final String table,
            final Function<String, Column> customColumnExpressions
    ) {
        System.out.println("Processing " + path);
        Dataset<Row> df = sqlContext.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("multiLine", true)
                .load(path);

        List<Function<String, Column>> columnExpressions =
                Stream.of(
                        customColumnExpressions,
                        field -> field.endsWith("Date") ? dateExpression2.apply(field) : null,
                        field -> field.endsWith("Id") ? intExpression.apply(field) : null,
                        defaultExpression
                )
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        StructType schema = df.schema();
        System.out.println(schema);
        List<Column> columnList = JavaConverters.seqAsJavaListConverter(schema)
                .asJava()
                .stream()
                .map(StructField::name)
                .map(field -> columnExpressions
                        .stream()
                        .map(it -> it.apply(field))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Converter not found for field " + field))
                        .as(field)
                ).collect(Collectors.toList());

        Dataset<Row> result = df
                .select(JavaConverters.asScalaBufferConverter(columnList).asScala())
                .as(table);

        result.write()
                .mode(SaveMode.Overwrite)
                .option("user", dbLogin)
                .option("password", dbPassword)
                .option("numPartition", "8")
                .option("truncate", "true")
                .jdbc(dbUrl, table, new Properties());
    }
}
