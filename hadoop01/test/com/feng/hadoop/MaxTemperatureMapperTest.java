package com.feng.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;
import static org.mockito.Mockito.*;


public class MaxTemperatureMapperTest {

	@Test
	public void testMapper() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		Text value = new Text("0024552199012546626568525");
		OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
		mapper.map(null, value, output, null);
		Text outputkey = new Text("190");
		IntWritable outputValue = new IntWritable(27);
		
		verify(output, never()).collect(outputkey, outputValue);
		
	}
}
