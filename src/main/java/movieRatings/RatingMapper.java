package movieRatings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import movieRatings.RatingCountJob;
import java.io.IOException;


public class RatingMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private IntWritable rating = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] parts = value.toString().split("\\t+");

        if (parts.length == 4) {
            try {
                int ratingValue = Integer.parseInt(parts[2]);

                if (ratingValue >= 1 && ratingValue <= 5) {
                    rating.set(ratingValue);
                    context.write(rating, one);
                }
            } catch (NumberFormatException e) {

            }
        }
    }
}