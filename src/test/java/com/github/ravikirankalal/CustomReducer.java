package com.github.ravikirankalal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by ravikiran.kalal on 27/08/16.
 */
public class CustomReducer extends Reducer<Text, LongWritable,Text, LongWritable> {
    Map<Text,LongWritable> output;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        output = new HashMap<Text, LongWritable>();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        Iterator<LongWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            iterator.next();
            count++;
        }
        if (output.containsKey(key)) {
            output.put(new Text(key),new LongWritable(output.get(key).get() + count));
        } else {
            output.put(new Text(key),new LongWritable(count));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text key:output.keySet()) {
            context.write(key,output.get(key));
        }
    }
}
