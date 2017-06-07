package pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<Object, Text, IntWritable, MovieRating> {

    private static final String DELIMITER = "::";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // UserID::MovieID::Rating::Timestamp
        String[] fields = value.toString().split(DELIMITER, -1);

        int movieId = Integer.parseInt(fields[1]);
        int rating = Integer.parseInt(fields[2]);

        MovieRating row = new MovieRating();
        row.setMovieId(movieId);
        row.setRating(rating);

        context.write(new IntWritable(movieId), row);
    }
}
