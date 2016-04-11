package com.feng.hadoop.wordcount;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.SplitSampler;

import com.feng.hadoop.group.MyPair;

public class WordCountMain {

	public static void main(String[] args) throws Exception {
		//hdfs://192.168.0.117:9000/user/root/test hdfs://192.168.0.117:9000/user/root/output/wordcount3
//		System.setProperty("hadoop.home.dir", "F:/study/hadoop/hadoop-2.6.0");
		Path outputPath = new Path("hdfs://192.168.0.117:9000/user/root/output/wordcount5");
		runJob();
//		getFile(outputPath);
	}
	
	public static void runJob() throws Exception {
		Path outputPath = new Path("hdfs://192.168.0.117:9000/user/root/output/wordcount5");
		Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
		boolean hasFile = hdfs.exists(outputPath);
		if(hasFile) {
			hdfs.delete(outputPath, true);
		}
		Job job = Job.getInstance();
	    job.setJarByClass(WordCountMain.class);
	    job.setMapperClass(WordCountMapper.class);
	    job.setCombinerClass(WordCountReduce.class);
	    job.setReducerClass(WordCountReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(3);  
	    FileInputFormat.setInputPaths(job, "hdfs://192.168.0.117:9000/user/root/test/card.txt");
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.0.117:9000/user/root/output/wordcount5"));
	    
//	    job.setPartitionerClass(TotalOrderPartitioner.class);
//	    SplitSampler<Text, Text> sampler = new SplitSampler<Text, Text>(3);
//        //����hadoop�ֲ�ʽ�����ļ����������
//        Path catchPath = new Path("hdfs://192.168.0.117:9000/user/root/partitionFile");
//        TotalOrderPartitioner.setPartitionFile(config, catchPath);
//        //�Զ���ɻ����ļ�
//        InputSampler.writePartitionFile(job, sampler);
//        URI partitionUri = new URI(catchPath.toString()+ "#_partitions");
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void getFile(Path path) throws Exception {
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        if ( fs.exists(path) )
        {
        	FileStatus[] stats = fs.listStatus(path);
        	for(FileStatus file : stats) {
        		FSDataInputStream is = fs.open(file.getPath());
                // get the file info to create the buffer
                FileStatus stat = fs.getFileStatus(file.getPath());
               // System.out.println(stat.getLen() + "----" + stat.getOwner());
                // create the buffer
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                is.readFully(0, buffer);
                String content = new String(buffer, "UTF-8");
                System.out.println(content);
                is.close();
                
        	}
        	fs.close();
        }
	}
}
