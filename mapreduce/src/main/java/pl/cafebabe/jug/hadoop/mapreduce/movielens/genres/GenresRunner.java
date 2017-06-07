package pl.cafebabe.jug.hadoop.mapreduce.movielens.genres;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// yarn jar mapreduce-1.0.jar pl.cafebabe.jug.hadoop.mapreduce.movielens.genres.GenresRunner /user/root/movielens/movies /user/root/wyniki-genres
public class GenresRunner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);

        Job job = Job.getInstance(conf, "movielens-genres-job");

        job.setJarByClass(GenresRunner.class);
        job.setMapperClass(GenresMapper.class);
        job.setReducerClass(GenresReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}