package com.feng.hadoop.group;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.SplitSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


public class MyPairMain {

	public static void main(String[] args) throws Exception {
		//hdfs://192.168.0.117:9000/user/root/test hdfs://192.168.0.117:9000/user/root/output/wordcount3
//		System.setProperty("hadoop.home.dir", "F:/study/hadoop/hadoop-2.6.0");
		Path outputPath = new Path("hdfs://192.168.0.117:9000/user/root/output/wordcount4");
		runJob();
//		getFile(outputPath);
	}
	
	public static void runJob() throws Exception {
		Path outputPath = new Path("hdfs://192.168.0.117:9000/user/root/output/wordcount4");
		Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
		boolean hasFile = hdfs.exists(outputPath);
		if(hasFile) {
			hdfs.delete(outputPath);
		}
		Job job = Job.getInstance();
	    job.setJarByClass(MyPairMain.class);
	    job.setMapperClass(WordCountMapper.class);
	    
	    job.setPartitionerClass(MyPartitioner.class);
	    
	    job.setGroupingComparatorClass(MyComparator.class);
//	    job.setCombinerClass(WordCountReduce.class);
	    job.setReducerClass(WordCountReduce.class);
	    // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // map 输出Key的类型
        job.setMapOutputKeyClass(MyPair.class);
        // map输出Value的类型
        job.setMapOutputValueClass(Text.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);
        
	    FileInputFormat.setInputPaths(job, "hdfs://192.168.0.117:9000/user/root/test/card.txt");
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.0.117:9000/user/root/output/wordcount4"));
	    
	    
	    SplitSampler<MyPair, Text> sampler = new SplitSampler<MyPair, Text>(3);
        //告诉hadoop分布式缓存文件放在哪里好
        Path catchPath = new Path("hdfs://192.168.0.117:9000/user/root/partitionFile");
        TotalOrderPartitioner.setPartitionFile(config, catchPath);
        //自动生成缓存文件
        InputSampler.writePartitionFile(job, sampler);
        URI partitionUri = new URI(catchPath.toString()+ "#_partitions");
        //添加到分布式缓存
        DistributedCache.addCacheFile(partitionUri, config);
        DistributedCache.createSymlink(config);
        
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
