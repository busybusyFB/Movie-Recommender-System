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

public class MovieSetCollector {
    public static class CorrectMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text line, Context context) throws InterruptedException, IOException {
            //"id,movie,rating" --> (movie 1)
            if (line.toString() == null) return;

            String[] strings = line.toString().split(",");

            if (strings.length != 3) return;

            String movie_id = strings[1];
            context.write(new Text(movie_id), new IntWritable(1));
        }
    }

    public static class CorrectReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable count: counts) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(CorrectMapper.class);
        job.setReducerClass(CorrectReducer.class);
        job.setJarByClass(MovieSetCollector.class);
        job.setJobName("movieCollector");

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
