package com.feng.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
//		StringTokenizer tokenizer = new StringTokenizer(line);
//		while(tokenizer.hasMoreTokens()) {
//			word.set(tokenizer.nextToken());
//			context.write(word, one);
//		}
		String[] lineArr = line.split("\t");
//		System.out.println(lineArr[0]);
//		myPair.setFirst(lineArr[4]);
//		myPair.setSecond(lineArr[2]);
		context.write(new Text(lineArr[4]), value);
	}

}
