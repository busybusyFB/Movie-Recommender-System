import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalizer {
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input value is: m1:m2, count
            String movieA = value.toString().split("\t")[0].split(":")[0];
            String movieB = value.toString().split("\t")[0].split(":")[1];
            String counts = value.toString().split("\t")[1];
            //output key: movieA
            //output value: movieB:counts
            context.write(new Text(movieA), new Text(movieB + ":" + counts));
        }
    }
    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            Map<String, Integer> map = new HashMap<String, Integer>();

            for (Text val : values) {
                String movieB = val.toString().split(":")[0];
                int count = Integer.parseInt(val.toString().split(":")[1]);
                sum += count;
                map.put(movieB, count);
            }

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String movieB = entry.getKey();
                int count = entry.getValue();
                double weight = (double) count / sum;
                String weightStr = String.valueOf(weight);
                context.write(new Text(movieB), new Text(key.toString() + "=" + weightStr));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setJarByClass(Normalizer.class);
        job.setJobName("normalize");

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
