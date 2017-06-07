package pl.cafebabe.jug.hadoop.mapreduce.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final String DELIMITERS = " \t\n\r\f,.:;![]()'*\"„”";

    private Text k2 = new Text();
    private IntWritable v2 = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = StringUtils.split(value.toString().toLowerCase(), DELIMITERS);
        for (String word : words) {
            // pomijamy najkrótsze słowa
            if (word.length() > 3) {
                k2.set(word);
                v2.set(1);
                context.write(k2, v2);
            }
        }
    }

}