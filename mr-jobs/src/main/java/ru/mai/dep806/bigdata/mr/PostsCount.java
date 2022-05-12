package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Takes StackOverflow posts as input and output post type - count
 * SQL: select postTypeId, count(*) from posts;
 * <p>
 * hadoop jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.PostsCount /user/stud/stackoverflow/landing/Posts
 * <p>
 * Example output:
 * Post Types
 *      ALL=37215531
 *      Answer=22668556
 *      Moderator nomination=295
 *      Orphaned tag wiki=167
 *      Privilege wiki=2
 *      Question=14458875
 *      Tag wiki=43814
 *      Tag wiki excerpt=43815
 *      Undefined=3
 *      Wiki placeholder=4
 */
public class PostsCount {

    private static final String POST_TYPE_COUNTER_GROUP = "Post Types";

    private static Map<String, String> postTypes = new HashMap<String, String>() {{
        put("1", "Question");
        put("2", "Answer");
        put("3", "Orphaned tag wiki");
        put("4", "Tag wiki excerpt");
        put("5", "Tag wiki");
        put("6", "Moderator nomination");
        put("7", "Wiki placeholder");
        put("8", "Privilege wiki");
        put("N/A", "Undefined");
    }};

    private static class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String postTypeId = row.get("PostTypeId");
            if (postTypeId == null) {
                postTypeId = "N/A";
            }
            String postTypeName = postTypes.get(postTypeId);
            context.getCounter(POST_TYPE_COUNTER_GROUP, postTypeName).increment(1);
            context.getCounter(POST_TYPE_COUNTER_GROUP, "ALL").increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        Path inputDir = new Path(args[0]);

        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(conf, "StackOverflow post's types counts");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(PostsCount.class);
        // Указываем класс Маппера
        job.setMapperClass(CountMapper.class);
        // Комбайнер и Редьюсер не используются
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(NullWritable.class);
        // Отключаем reduce
        job.setNumReduceTasks(0);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, inputDir);
        // Отключаем вывод файла (результат ожидается только в счетчиках)
        job.setOutputFormatClass(NullOutputFormat.class);
        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса

        if (success) {
            // Если задача завершена успешно, печатаем счетчики на консоль
            for (Counter counter : job.getCounters().getGroup(POST_TYPE_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + ": " + counter.getValue());
            }
        }
        System.exit(success ? 0 : 1);
    }
}
