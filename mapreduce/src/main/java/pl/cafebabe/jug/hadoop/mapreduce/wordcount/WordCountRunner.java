package pl.cafebabe.jug.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// yarn jar mapreduce-1.0.jar pl.cafebabe.jug.hadoop.mapreduce.wordcount.WordCountRunner /user/root/lektury /user/root/wyniki-wc
public class WordCountRunner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        // ewentualne usunięcie plików wyjściowych
        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);

        // utworzenie zadania o określonej nazwie
        Job job = Job.getInstance(conf, "word-count-job");

        // najważniejsza część (sic!): ustawiamy mapera i reduktora
        job.setJarByClass(WordCountRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // określamy typy wyjściowe z reduktora (zamazane typy generyczne)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // wejście i wyjście
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // i wreszcie uruchomienie
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}