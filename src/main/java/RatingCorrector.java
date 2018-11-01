import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingCorrector {
    public static class RawInputMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text line, Context context) throws InterruptedException, IOException {
            //"id,movie,rating" --> (id   movie:rating)
            if (line.toString() == null) return;

            String[] strings = line.toString().split(",");

            if (strings.length != 3) return;

            int user_id = Integer.parseInt(strings[0]);
            String movie_rating = strings[1] + ":" + strings[2];
            context.write(new IntWritable(user_id), new Text(movie_rating));
        }
    }

    public static class MovieSetMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text line, Context context) throws InterruptedException, IOException {
            //input: "movie   \t   counts" --> (movie counts)
            String[] strings = line.toString().trim().split("\t");

        }
    }

    public static class CorrectReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int ratedNum = 0;
            for(IntWritable count: counts) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
