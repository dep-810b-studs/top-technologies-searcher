package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.Map;

/**
 * Generate Bloom filter for Users with reputation > N.
 * <p>
 * hadoop jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.HighReputationUsersBloomFilter /user/stud/stackoverflow/landing/users /user/stud/eugene/highrepusers.bloom /user/stud/stackoverflow/landing/posts /user/stud/eugene/high-rep-user-posts
 * select * from posts where ownerId in (select id from users where reputation > 1000)
 */
public class HighReputationUsersBloomFilter {

    private static final String BLOOM_FILE = "bloomFile";

    private static class GenerateBloomFilterMapper extends Mapper<Object, Text, NullWritable, BloomFilter> {

        private BloomFilter filter = createBloomFilter();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String reputationString = row.get("Reputation");
            if (reputationString != null && Integer.parseInt(reputationString) > 1000) {
                filter.add(new Key(row.get("Id").getBytes()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), filter);
        }
    }

    private static class GenerateBloomFilterReducer
            extends Reducer<NullWritable, BloomFilter, NullWritable, NullWritable> {

        private BloomFilter filter = createBloomFilter();

        @Override
        protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            for (BloomFilter value : values) {
                filter.or(value);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Path bloomFile = new Path(context.getConfiguration().get(BLOOM_FILE));
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(bloomFile)) {
                fs.delete(bloomFile, false);
            }
            try (FSDataOutputStream out = fs.create(bloomFile)) {
                filter.write(out);
            }
        }
    }

    private static class BloomFilterMapper extends Mapper<Object, Text, NullWritable, Text> {

        private BloomFilter filter = new BloomFilter();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path bloomFile = new Path(context.getConfiguration().get(BLOOM_FILE));
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (FSDataInputStream is = fs.open(bloomFile)) {
                filter.readFields(is);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            // Test that element included in filter set
            String authorId = row.get("AuthorId");
            if (StringUtils.isNotBlank(authorId) &&
                    filter.membershipTest(new Key(authorId.getBytes()))) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    private static BloomFilter createBloomFilter() {
        int numMembers = 1000;
        float falsePositiveProbability = 0.1f;

        int bitmaskSize = getOptimalBloomFilterSize(numMembers, falsePositiveProbability);
        int nbHash = getOptimalK(numMembers, bitmaskSize);

        return new BloomFilter(bitmaskSize, nbHash, Hash.MURMUR_HASH);
    }

    private static int getOptimalBloomFilterSize(int numRecords, float falsePosRate) {
        return (int) (-numRecords * (float) Math.log(1.0 - falsePosRate) / Math.pow(Math.log(2), 2));
    }

    private static int getOptimalK(float numMembers, float vectorSize) {
        return (int) Math.round(vectorSize / numMembers * Math.log(2));
    }

    public static void main(String[] args) throws Exception {
        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(conf, "Generate Bloom filter for High Reputation Users");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(HighReputationUsersBloomFilter.class);
        // Указываем класс Маппера
        job.setMapperClass(GenerateBloomFilterMapper.class);
        // Редьюсер
        job.setReducerClass(GenerateBloomFilterReducer.class);
        // Количество редьюсеров = 1, чтобы получился один файл с фильтром
        job.setNumReduceTasks(1);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(BloomFilter.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Выходного файла нет
        job.setOutputFormatClass(NullOutputFormat.class);
        // Путь к файлу на выход (куда запишутся результаты)
        job.getConfiguration().set(BLOOM_FILE, args[1]);
        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);

        if (success) {
            Job filterJob = Job.getInstance(conf, "Filter Posts by Bloom filter");
            // Указываем архив с задачей по имени класса в этом архиве
            filterJob.setJarByClass(HighReputationUsersBloomFilter.class);
            // Указываем класс Маппера
            filterJob.setMapperClass(BloomFilterMapper.class);
            // Тип ключа на выходе
            filterJob.setOutputKeyClass(NullWritable.class);
            // Тип значения на выходе
            filterJob.setOutputValueClass(Text.class);
            // Путь к файлу на вход
            FileInputFormat.addInputPath(filterJob, new Path(args[2]));
            // Путь к файлу на выход (куда запишутся результаты)
            FileOutputFormat.setOutputPath(filterJob, new Path(args[3]));
            filterJob.getConfiguration().set(BLOOM_FILE, args[1]);
            // Запускаем джобу и ждем окончания ее выполнения
            success = filterJob.waitForCompletion(true);
        }
        // Возвращаем ее статус в виде exit-кода процесса
        System.exit(success ? 0 : 1);
    }

}
