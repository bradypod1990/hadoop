package com.feng.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, Text, Text, Text> {

	private IntWritable count = new IntWritable(0);
	private static final Text SEPARATOR = new Text("---------");
    
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		context.write(SEPARATOR, new Text(""));
		for(Text value : values) {
//			sum += value.get();
			context.write(key, value);
		}
//		count.set(sum);
//		context.write(key, count);
	}
}
