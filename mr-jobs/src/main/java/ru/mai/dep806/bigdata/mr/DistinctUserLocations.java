package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Takes StackOverflow users as input and output distinct locations.
 * SQL: select distinct trim(location) from user;
 */
public class DistinctUserLocations {

    private static class LocationMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String location = row.get("Location");
            if (StringUtils.isNotBlank(location)) {
                outKey.set(location.trim());
                context.write(outKey, NullWritable.get());
            }
        }
    }

    private static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(conf, "StackOverflow user's distinct locations");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(DistinctUserLocations.class);
        // Указываем класс Маппера
        job.setMapperClass(LocationMapper.class);
        // Класс Комбайнера
        job.setCombinerClass(DistinctReducer.class);
        // Редьюсера - одна и та же реализация для слияния изменений
        job.setReducerClass(DistinctReducer.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(Text.class);
        // Тип значения на выходе
        job.setOutputValueClass(NullWritable.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        System.exit(success ? 0 : 1);
    }
}
