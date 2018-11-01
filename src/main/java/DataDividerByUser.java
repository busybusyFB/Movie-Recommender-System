import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataDividerByUser {
    public static class DataDivideMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {
        @Override
        public void map(LongWritable key, Text line, Context context) throws InterruptedException, IOException {
            //"id,movie,rating" --> (id, movie:rating)
            if (line.toString() == null) return;

            String[] strings = line.toString().split(",");

            if (strings.length != 3) return;

            int user_id = Integer.parseInt(strings[0]);
            String movie_rating = strings[1] + ":" + strings[2];
            context.write(new IntWritable(user_id), new Text(movie_rating));
        }
    }

    public static class DataDivideReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            //(id, movie1:rate1)...(id, movie2:rate2) --> key = id, value = "movie1:rate1, movie2, rate2, ..."
            StringBuilder sb = new StringBuilder();
            for(Text val : values) {
                sb.append(",");
                sb.append(val.toString());
            }
            context.write(key, new Text(sb.toString().replaceFirst(",", "")));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(DataDivideMapper.class);
        job.setReducerClass(DataDivideReducer.class);
        job.setJarByClass(DataDividerByUser.class);
        job.setJobName("datadivider");

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
