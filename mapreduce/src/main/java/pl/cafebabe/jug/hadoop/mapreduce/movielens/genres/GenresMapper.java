package pl.cafebabe.jug.hadoop.mapreduce.movielens.genres;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// yarn yar
public class GenresMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text k2 = new Text();
    private IntWritable v2 = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // MovieID::Title::Genres, np. 84::Last Summer in the Hamptons (1995)::Comedy|Drama
        String[] fields = value.toString().split("::", -1);
        String[] genres = fields[2].split("\\|");
        for (String genre : genres) {
            k2.set(genre);
            context.write(k2, v2);
        }
    }
}
