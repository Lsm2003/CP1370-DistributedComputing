package movieRatings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RatingCountJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RatingCountJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rating Count");

        // Set the Jar by class
        job.setJarByClass(RatingCountJob.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        // Set the output types
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths
        TextInputFormat.addInputPath(job, new Path(args[0]));  // input file
        TextOutputFormat.setOutputPath(job, new Path(args[1])); // output file

        // Submit the job and wait for it to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}