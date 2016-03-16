package com.feng.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends
		MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

//	@Override
//	protected void map(LongWritable key, Text value,
//			Context context)
//			throws IOException, InterruptedException {
//		
//		String line = value.toString();
//		String year = line.substring(15,19);
//		int temperature = Integer.parseInt(line.substring(87, 92));
//		context.write(new Text(year), new IntWritable(temperature));
//	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String year = line.substring(7,10);
		int temperature = Integer.parseInt(line.substring(17, 18));
		output.collect(new Text(year), new IntWritable(temperature));
	}

}
