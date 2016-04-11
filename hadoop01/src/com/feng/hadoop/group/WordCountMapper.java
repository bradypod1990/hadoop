package com.feng.hadoop.group;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, MyPair, Text> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private MyPair myPair = new MyPair();
	
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArr = line.split("\t");
//		System.out.println(lineArr[0]);
		myPair.setFirst(lineArr[4]);
		myPair.setSecond(lineArr[2]);
		context.write(myPair, value);
//		StringTokenizer tokenizer = new StringTokenizer(line);
//		while(tokenizer.hasMoreTokens()) {
////			word.set(tokenizer.nextToken());
//			myPair.setFirst(tokenizer.nextToken());
//			myPair.setSecond("1");
//			context.write(myPair, new Text("1"));
//		}
	}

}
