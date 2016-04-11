package com.feng.hadoop.group;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<MyPair, Text, Text, Text> {

	private IntWritable count = new IntWritable(0);
	private static final Text SEPARATOR = new Text("---------");
	@Override
	protected void reduce(MyPair key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		context.write(SEPARATOR, new Text(""));
		for(Text value : values) {
//			sum += Integer.parseInt(value.toString());
			context.write(new Text(key.getFirst()), value);
		}
//		count.set(sum);
//		context.write(new Text(key.getFirst()), new Text(count.get()+""));
	}
}
