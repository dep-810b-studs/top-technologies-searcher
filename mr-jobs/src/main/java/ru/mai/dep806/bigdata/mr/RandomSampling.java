package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;

/**
 * Фильтрует входные данные (посты): выбирает случайные строки с заданной вероятностью.
 */
public class RandomSampling extends Configured implements Tool {

    private static class SamplingMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Random random = new Random();
        private double probability;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            probability = context.getConfiguration().getDouble("probability", 0.1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (random.nextDouble() < probability) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    private static class ValuePartitioner extends Partitioner<NullWritable, Text> {

        @Override
        public int getPartition(NullWritable nullWritable, Text text, int numPartitions) {
            return (text.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        double probability = Double.parseDouble(args[2]) / 100.0;

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Take random " + probability + " of " + inputPath);
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(RandomSampling.class);
        // Мэппер
        job.setMapperClass(SamplingMapper.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Пути к входным файлам, формат файла и мэппер
        FileInputFormat.addInputPath(job, inputPath);

        job.setPartitionerClass(ValuePartitioner.class);

        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().setDouble("probability", probability);
        TextInputFormat.getMinSplitSize(job);

        // Считаем кол-во reduce задач на основе кол-ва входных данных и процента
        TextInputFormat inputFormat = new TextInputFormat();
        int numInputSplits = inputFormat.getSplits(job).size();
        int numReduceTasks = (int) Math.ceil(probability * numInputSplits);
        job.setNumReduceTasks(numReduceTasks);

        System.out.println("Sampling " + inputPath + " with probability " +
                probability + " and output to " + outputPath +
                " with " + numReduceTasks + " reducers");

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new RandomSampling(), args);

        System.exit(result);
    }

}
