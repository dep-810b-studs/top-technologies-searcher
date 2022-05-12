package ru.mai.dep806.bigdata.mr;

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
import java.util.TreeMap;

/**
 * Top N Users by reputation.
 * select * from users order by reputation desc limit 10;
 */
public class TopNUsers {


    private static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> topNMap = new TreeMap<>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String reputationString = row.get("Reputation");
            if (reputationString != null) {
                topNMap.put(Integer.parseInt(reputationString), new Text(value));

                if (topNMap.size() > 10) {
                    topNMap.remove(topNMap.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : topNMap.values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    private static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> topNMap = new TreeMap<>();

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

                String reputationString = row.get("Reputation");
                if (reputationString != null) {
                    topNMap.put(Integer.parseInt(reputationString), new Text(value));

                    if (topNMap.size() > 10) {
                        topNMap.remove(topNMap.firstKey());
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : topNMap.descendingMap().values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(conf, "Top N Users");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(TopNUsers.class);
        // Указываем класс Маппера
        job.setMapperClass(TopNMapper.class);
        // Комбайнер не нужен
        // Редьюсер
        job.setReducerClass(TopNReducer.class);
        // Обязательно поставить кол-во редьюсеров 1,
        // иначе будет не один top N список, а несколько
        job.setNumReduceTasks(1);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
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
