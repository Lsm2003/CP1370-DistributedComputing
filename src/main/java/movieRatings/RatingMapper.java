package movieRatings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    // Output key: rating, value: 1 (for counting occurrences)
    private final static IntWritable one = new IntWritable(1);
    private IntWritable rating = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by whitespace
        String[] parts = value.toString().split("\\s+");

        // We are interested in the 3rd column, which is the rating (index 2)
        if (parts.length == 4) {
            try {
                int ratingValue = Integer.parseInt(parts[2]);
                // Ensure the rating is between 1 and 5
                if (ratingValue >= 1 && ratingValue <= 5) {
                    rating.set(ratingValue);
                    context.write(rating, one);  // Emit (rating, 1)
                }
            } catch (NumberFormatException e) {
                // Ignore invalid ratings
            }
        }
    }
}