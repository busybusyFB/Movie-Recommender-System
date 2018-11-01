import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
    public static class MultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input value(movieB  \t   movieA=weight)
            String outputKey = value.toString().split("\t")[0];
            String outputValue = value.toString().split("\t")[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //"id,movie,rating" --> (id, movie:rating)
            String[] fields = value.toString().split(",");
            //output key: movie
            //output value: id:rating
            context.write(new Text(fields[1]), new Text(fields[0] + ":" + fields[2]));
        }
    }

    public static class MultiplyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Double> userRate = new HashMap<String, Double>();
            Map<String, Double> movieWeight = new HashMap<String, Double>();

            for (Text value : values) {
                String str = value.toString();
                if (str.contains("=")) {
                    movieWeight.put(str.split("=")[0], Double.parseDouble(str.split("=")[1]));
                } else {
                    userRate.put(str.split(":")[0],Double.parseDouble(str.split(":")[1]));
                }
            }

            for(Map.Entry<String, Double> entry : userRate.entrySet()) {

                String user = entry.getKey();
                double rate = entry.getValue();

                for (Map.Entry<String, Double> entry1 : movieWeight.entrySet()) {

                    String movie = entry1.getKey();
                    double weight = entry1.getValue();

                    String user_movie = user + ":" + movie;
                    double subrating = weight * rate;

                    //output key: user:movie
                    //output value: subrating
                    context.write(new Text(user_movie), new Text(String.valueOf(subrating)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(RatingMapper.class);
        job.setMapperClass(MultiplyMapper.class);
        job.setReducerClass(MultiplyReducer.class);
        job.setJarByClass(Multiplication.class);
        job.setJobName("multiplication");

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultiplyMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
