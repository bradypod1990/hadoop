package com.feng.hadoop.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyPair, Text> {

	@Override
	public int getPartition(MyPair key, Text value, int numPartitions) {
		
		return Math.abs(key.getFirst().hashCode()*127) % numPartitions;
	}

}
