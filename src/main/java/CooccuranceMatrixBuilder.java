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

public class CooccuranceMatrixBuilder {
    public static class MatrixBuilderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //id  m1,r1,m2,r2... --> (m1:m2, 1) (m1:m3, 1); ....
            String[] pairs = value.toString().trim().split("\t")[1].split(",");

            for (int i = 0; i < pairs.length; i++) {
                String movieA = pairs[i].trim().split(":")[0];
                for (int j = 0; j < pairs.length; j++) {
                    String movieB = pairs[j].trim().split(":")[0];
                    context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
                }
            }
        }
    }
    public static class MatrixBuilderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable count: counts) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(MatrixBuilderMapper.class);
        job.setReducerClass(MatrixBuilderReducer.class);
        job.setJarByClass(CooccuranceMatrixBuilder.class);
        job.setJobName("cooccurancematrix");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
