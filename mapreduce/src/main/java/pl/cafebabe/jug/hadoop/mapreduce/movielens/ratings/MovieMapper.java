package pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<Object, Text, IntWritable, MovieRating> {

    private static final String DELIMITER = "::";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // MovieID::Title::Genres
        String[] fields = value.toString().split(DELIMITER, -1);
        int movieId = Integer.parseInt(fields[0]);
        String title = fields[1];

        MovieRating row = new MovieRating();
        row.setMovieId(movieId);
        row.setTitle(title);

        context.write(new IntWritable(movieId), row);
    }
}
