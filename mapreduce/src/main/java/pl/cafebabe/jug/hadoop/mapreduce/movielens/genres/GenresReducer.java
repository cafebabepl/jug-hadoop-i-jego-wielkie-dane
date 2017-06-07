package pl.cafebabe.jug.hadoop.mapreduce.movielens.genres;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class GenresReducer extends IntSumReducer<Text> {
}
