package pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingsReducer extends Reducer<IntWritable, MovieRating, IntWritable, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<MovieRating> values, Context context) throws IOException, InterruptedException {
        String title = null;
        int count = 0;
        int sum = 0;
        double rating = 0.0;

        for (MovieRating row : values) {
            if (StringUtils.isNotBlank(row.getTitle())) {
                title = row.getTitle();
            } else {
                sum += row.getRating();
                count++;
            }
        }

        // wyliczenie średniej oceny
        if (count > 0) {
            rating = (double) sum / count;
        }

        // pomijamy filmy z małą liczbą ocen
        if (count > 100) {
            result.set(String.format("%s\t%d\t%.3f", title, count, rating));
            context.write(key, result);
        }
    }
}
