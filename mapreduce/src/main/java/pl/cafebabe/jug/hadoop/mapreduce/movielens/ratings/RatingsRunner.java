package pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pl.cafebabe.jug.hadoop.mapreduce.wordcount.WordCountMapper;
import pl.cafebabe.jug.hadoop.mapreduce.wordcount.WordCountReducer;

// yarn jar mapreduce-1.0.jar pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings.RatingsRunner /user/root/movielens/movies /user/root/movielens/ratings /user/root/wyniki-ratings
public class RatingsRunner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path moviesInput = new Path(args[0]);
        Path ratingsInput = new Path(args[1]);
        Path output = new Path(args[2]);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);

        Job job = Job.getInstance(conf, "movielens-ratings-job");

        job.setJarByClass(RatingsRunner.class);
        job.setReducerClass(RatingsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MovieRating.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, moviesInput, TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, ratingsInput, TextInputFormat.class, RatingMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}