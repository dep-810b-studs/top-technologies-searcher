package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Фильтрует входные данные (посты): выбирает только вопросы по указанным тегам за последний год.
 */
public class RecentPostsByTag extends Configured implements Tool {

    private static class FilterMapper extends Mapper<Object, Text, NullWritable, Text> {

        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder(250);
        private final String[] fields = new String[] {
                "Id", "PostTypeId", "AcceptedAnswerId", "ParentId" , "CreationDate", "DeletionDate",
                "Score", "ViewCount", "Body" , "OwnerUserId", "OwnerDisplayName", "LastEditorUserId",
                "LastEditorDisplayName", "LastEditDate", "LastActivityDate", "Title", "Tags", "AnswerCount",
                "CommentCount", "FavoriteCount", "ClosedDate", "CommunityOwnedDate"
        };
        private List<String> searchTags = null;
        private final Date cutOffDate = Date.from(java.time.LocalDateTime.now().minusYears(1).toInstant(java.time.ZoneOffset.UTC));

        @Override
        protected void setup(Context context) {
            searchTags = Arrays.stream(context.getConfiguration().get("tags").split(","))
                    .map(tag -> "<" + tag.trim() + ">")
                    .collect(Collectors.toList());
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            if (IsQuestion(row) && hasAtLeastOneTag(row) && createdAfter(row, cutOffDate)) {
                outValue.set(concatenateFields(row));
                context.write(NullWritable.get(), outValue);
            }
        }

        private String concatenateFields(Map<String, String> row) {
            buffer.setLength(0);
            for (String field : fields) {
                String fieldValue = row.get(field);
                if (fieldValue != null) {
                    buffer.append(fieldValue);
                }
                buffer.append('\01');
            }
            return buffer.toString();
        }

        private boolean createdAfter(Map<String, String> row, Date cutOffDate) {
            String creationDateString = row.get("CreationDate");
            try {
                Date creationDate = dateFormat.parse(creationDateString);
                return creationDate.after(cutOffDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return false;
        }

        private boolean hasAtLeastOneTag(Map<String, String> row) {
            // &lt;html&gt; -> <html>
            String tags = StringEscapeUtils.unescapeHtml(row.get("Tags"));
            for (String searchTag : searchTags) {
                if (tags.contains(searchTag)) {
                    return true;
                }
            }
            return false;
        }

        private boolean IsQuestion(Map<String, String> row) {
            return "1".equals(row.get("PostTypeId"));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String tags = args[2];

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Filter recent questions by tags");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(RecentPostsByTag.class);
        // Указываем класс Маппера
        job.setMapperClass(FilterMapper.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Включаем компрессию
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.setNumReduceTasks(0);

        // Передаем параметры в конфигурацию задачи
        job.getConfiguration().set("tags", tags);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 2;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new RecentPostsByTag(), args);

        System.exit(result);
    }

}
