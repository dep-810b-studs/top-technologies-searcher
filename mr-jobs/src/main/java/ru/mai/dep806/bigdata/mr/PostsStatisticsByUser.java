package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Post Statistics per owner user: posts count, min, max and avg score.
 * select ownerdisplayname, count(*), min(score), max(score), avg(score) from posts group by ownerdisplayname.
 *
 * hadoop jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.PostsStatisticsByUser /user/stud/stackoverflow/landing/Posts /user/stud/eugene/PostStats
 */
public class PostsStatisticsByUser {

    static class PostsStats implements Writable {
        Integer minScore = 0;
        Integer maxScore = 0;
        int sumScore = 0;
        int count = 0;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(minScore);
            out.writeInt(maxScore);
            out.writeInt(sumScore);
            out.writeInt(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            minScore = in.readInt();
            maxScore = in.readInt();
            sumScore = in.readInt();
            count = in.readInt();
        }

        Integer getMinScore() {
            return minScore;
        }

        void setMinScore(Integer minScore) {
            this.minScore = minScore;
        }

        Integer getMaxScore() {
            return maxScore;
        }

        void setMaxScore(Integer maxScore) {
            this.maxScore = maxScore;
        }

        int getSumScore() {
            return sumScore;
        }

        void setSumScore(int sumScore) {
            this.sumScore = sumScore;
        }

        int getCount() {
            return count;
        }

        void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return String.format("%d\t%d\t%d\t%f", count, minScore, maxScore, ((double) sumScore) / count);
        }
    }

    private static class UserPostStatsMapper extends Mapper<Object, Text, Text, PostsStats> {

        private Text outKey = new Text();
        private PostsStats outValue = new PostsStats();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String owner = row.get("OwnerDisplayName");
            if (StringUtils.isNotBlank(owner)) {
                outKey.set(owner);
                String scoreString = row.get("Score");
                outValue.setCount(1);

                if (StringUtils.isNotBlank(scoreString)) {
                    int score = Integer.parseInt(scoreString);
                    outValue.setMinScore(score);
                    outValue.setMaxScore(score);
                    outValue.setSumScore(score);
                }
                context.write(outKey, outValue);
            }
        }
    }

    private static class StatsReducer extends Reducer<Text, PostsStats, Text, PostsStats> {

        private PostsStats result = new PostsStats();

        @Override
        protected void reduce(Text key, Iterable<PostsStats> values, Context context) throws IOException, InterruptedException {
            result.setCount(0);
            result.setSumScore(0);
            result.setMaxScore(null);
            result.setMinScore(null);

            for (PostsStats stats : values) {
                result.setCount( result.getCount() + stats.getCount() );
                result.setSumScore( result.getSumScore() + stats.getSumScore() );
                if (result.getMinScore() == null || stats.getMinScore() < result.getMinScore()) {
                    result.setMinScore(stats.getMinScore());
                }
                if (result.getMaxScore() == null || stats.getMaxScore() > result.getMaxScore()) {
                    result.setMaxScore(stats.getMaxScore());
                }
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(conf, "StackOverflow user's posts count and score statistics");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(PostsStatisticsByUser.class);
        // Указываем класс Маппера
        job.setMapperClass(UserPostStatsMapper.class);
        // Класс Комбайнера
        job.setCombinerClass(StatsReducer.class);
        // Редьюсера - одна и та же реализация для слияния изменений
        job.setReducerClass(StatsReducer.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(Text.class);
        // Тип значения на выходе
        job.setOutputValueClass(PostsStats.class);
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
