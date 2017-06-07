package pl.cafebabe.jug.hadoop.mapreduce.movielens.ratings;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieRating implements WritableComparable<MovieRating> {

    private int movieId;
    // żeby pominąć błąd serializacji Error: java.lang.NullPointerException at java.io.DataOutputStream.writeUTF(DataOutputStream.java:347)
    private String title = StringUtils.EMPTY;
    private int rating;

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = StringUtils.defaultString(title);
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    @Override
    public int compareTo(MovieRating o) {
        return Integer.compare(this.movieId, o.getMovieId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.movieId);
        out.writeUTF(this.title);
        out.writeInt(this.rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieId = in.readInt();
        this.title = in.readUTF();
        this.rating = in.readInt();
    }
}
