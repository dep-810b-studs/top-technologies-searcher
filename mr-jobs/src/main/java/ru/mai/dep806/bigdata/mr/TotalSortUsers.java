package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.util.Map;

/**
 * Takes StackOverflow users as input and output sorted by create date.
 * SQL: select * from user order by CreationDate;
 * hadoop jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.TotalSortUsers /user/stud/stackoverflow/landing/Users /user/stud/eugene/sorted-users
 */
public class TotalSortUsers {

    private static class CreateDateMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String creationDate = row.get("CreationDate");
            outKey.set(creationDate == null ? "" : creationDate);
            context.write(outKey, value);
        }
    }

    private static class ValueReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path partitionsFile = new Path(args[1] + "-partitions");
        Path stagingPath = new Path(args[1] + "-stage");

        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job analysisJob = Job.getInstance(conf, "StackOverflow Users sort: Stage 1/2 Analysis");
        // Указываем архив с задачей по имени класса в этом архиве
        analysisJob.setJarByClass(TotalSortUsers.class);
        // Указываем класс Маппера
        analysisJob.setMapperClass(CreateDateMapper.class);
        // Партишнера нет, устанавливаем кол-во партишнеров в 0
        analysisJob.setNumReduceTasks(0);
        // Тип ключа на выходе
        analysisJob.setOutputKeyClass(Text.class);
        // Тип значения на выходе
        analysisJob.setOutputValueClass(Text.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(analysisJob, inputPath);
        // Промежуточный файл записываем в Sequence файл, который сохраняет ключ отдельно
        // чтобы не извлекать заново ключ на следующем этапе
        analysisJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Путь к файлу на выход (куда запишутся результаты)
        SequenceFileOutputFormat.setOutputPath(analysisJob, stagingPath);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = analysisJob.waitForCompletion(true);

        if (success) {
            Job sortJob = Job.getInstance(conf, "StackOverflow Users sort: Stage 2/2 Sort");
            sortJob.setJarByClass(TotalSortUsers.class);
            // Identity Мэппер, который выдает вход на выход и ничего не делает
            sortJob.setMapperClass(Mapper.class);
            // Наш редьюсер
            sortJob.setReducerClass(ValueReducer.class);
            sortJob.setNumReduceTasks(14);
            // Самое главное: Указываем партишнер,
            // который использует файл с диапазонами значений ключа
            sortJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(sortJob.getConfiguration(), partitionsFile);
            // Типы выходных ключа и значения
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(Text.class);

            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(sortJob, stagingPath);

            TextOutputFormat.setOutputPath(sortJob, outputPath);

            // Этап 2
            // На основании входного файла, в partitionsFile записывается
            // случайная (c p=0.001) выборка из 10000 значений ключа составленная из (максимум) 10 HDFS-блоков входных данных
            InputSampler.RandomSampler<Object, Object> sampler = new InputSampler.RandomSampler<>(0.001, 10000, 10);
            // отключим компрессию partitionFile, чтобы можно было его посмотреть
            sortJob.getConfiguration().set("io.seqfile.compression.type", SequenceFile.CompressionType.NONE.name());
            InputSampler.writePartitionFile(sortJob, sampler);

            // Этап 3
            // Запуска сортировки
            success = sortJob.waitForCompletion(true);
        }

        // Удаляем промежуточные файлы
        FileSystem fileSystem = FileSystem.get(new Configuration());
        // fileSystem.delete(partitionsFile, false);
        fileSystem.delete(stagingPath, true);

        // Возвращаем ее статус в виде exit-кода процесса
        System.exit(success ? 0 : 1);
    }
}
