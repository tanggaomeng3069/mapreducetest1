package com.younger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
