package ru.mai.dep806.bigdata.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static ru.mai.dep806.bigdata.mr.SequenceFileUtils.toSequenceString;

/**
 * Соединяет (Join) Posts и Users по автору (OwnerUserId).
 * Вариант с использованием GenericWritable вместо префикса типа таблицы.
 */
public class PostsUsersJoinTyped extends Configured implements Tool {

    static final String[] POST_FIELDS = new String[]{
            "Id", "PostTypeId", "AcceptedAnswerId", "ParentId", "CreationDate", "DeletionDate",
            "Score", "ViewCount", "OwnerUserId", "OwnerDisplayName", "LastEditorUserId",
            "LastEditorDisplayName", "LastEditDate", "LastActivityDate", "Title", "Tags", "AnswerCount",
            "CommentCount", "FavoriteCount", "ClosedDate", "CommunityOwnedDate"
    };

    static final String[] USER_FIELDS = new String[]{
            "Id", "Reputation", "CreationDate", "DisplayName", "LastAccessDate", "Location", "Views",
            "UpVotes", "DownVotes", "Age", "AccountId"
    };

    enum RecordType {NA, Post, User}
    private static class TextWithType implements Writable {

        // Пустой конструктор нужен для десериализации фреймворком
        public TextWithType() {
            this(RecordType.NA);
        }

        TextWithType(RecordType recordType) {
            this.recordType = recordType;
            this.record = new Text();
        }

        private RecordType recordType;
        private Text record;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(recordType.ordinal());
            record.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            recordType = RecordType.values()[in.readInt()];
            record.readFields(in);
        }

        RecordType getRecordType() {
            return recordType;
        }

        Text getRecord() {
            return record;
        }
    }

    private static class PostsMapper extends Mapper<Object, Text, LongWritable, TextWithType> {
        private LongWritable outKey = new LongWritable();
        private TextWithType outValue = new TextWithType(RecordType.Post);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String keyString = row.get("OwnerUserId");

            if (StringUtils.isNotBlank(keyString)) {
                outKey.set(Long.parseLong(keyString));
                outValue.getRecord().set(toSequenceString(row, POST_FIELDS));
                context.write(outKey, outValue);
            }
        }
    }

    private static class UsersMapper extends Mapper<Object, Text, LongWritable, TextWithType> {
        private LongWritable outKey = new LongWritable();
        private TextWithType outValue = new TextWithType(RecordType.User);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String keyString = row.get("Id");
            if (StringUtils.isNotBlank(keyString)) {
                outKey.set(Long.parseLong(keyString));
                outValue.getRecord().set(toSequenceString(row, USER_FIELDS));
                context.write(outKey, outValue);
            }
        }
    }

    static class JoinReducer extends Reducer<LongWritable, TextWithType, LongWritable, Text> {

        private Text outValue = new Text();
        private StringBuilder buffer = new StringBuilder();

        @Override
        protected void reduce(LongWritable key, Iterable<TextWithType> values, Context context) throws IOException, InterruptedException {

            List<String> users = new ArrayList<>();
            List<String> posts = new ArrayList<>();

            // Распределим значения по типам строк в соотв. списки
            for (TextWithType value : values) {
                String strValue = value.getRecord().toString();
                switch (value.getRecordType()) {
                    case Post:
                        posts.add(strValue);
                        break;
                    case User:
                        users.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + value.getRecordType());
                }
            }

            // Если с обеих сторон есть строки для данного ключа (inner join)
            if (posts.size() > 0 && users.size() > 0) {
                // Выполним Join
                for (String user : users) {
                    for (String post : posts) {
                        buffer.setLength(0);
                        buffer.append(post).append(user);
                        // (разделитель между полями Post и User уже есть в конце Post.
                        outValue.set(buffer.toString());
                        context.write(key, outValue);
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath1 = new Path(args[0]);
        Path inputPath2 = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Join Posts and Users");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(PostsUsersJoinTyped.class);
        // Указываем класс Редьюсера
        job.setReducerClass(JoinReducer.class);
        // Кол-во тасков
        job.setNumReduceTasks(10);
        // Тип ключа Mapper-а на выходе
        job.setMapOutputKeyClass(LongWritable.class);
        // Тип значения Mapper-а на выходе
        job.setMapOutputValueClass(TextWithType.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(LongWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Пути к входным файлам, формат файла и мэппер
        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, PostsMapper.class);
        MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, UsersMapper.class);
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Включаем компрессию
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new PostsUsersJoinTyped(), args);

        System.exit(result);
    }

}
