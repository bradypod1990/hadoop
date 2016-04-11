package com.feng.hadoop.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTable {

	
	public static class MyMap extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] childAndParent = value.toString().split(" ");
			if(childAndParent != null && childAndParent.length == 2 && !childAndParent[0].equals("child")) {
				String child = childAndParent[0];
				String parent = childAndParent[1];
				context.write(new Text(child) , new Text("1#" + child + "#" + parent));
				context.write(new Text(parent) , new Text("2#" + child + "#" + parent));
			}
		}
	}
	
	public static class MyReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			List<String> grandchild = new ArrayList<String>();
			List<String> grandparent = new ArrayList<String>();
			System.out.println("--------------------------");
			for(Text text : values) {
				System.out.println(text.toString());
				String[] str = text.toString().split("#");
				if(str != null && str.length == 3) {
					if("2".equals(str[0])) {
						grandchild.add(str[1]);
					}else if("1".equals(str[0])){
						grandparent.add(str[2]);
					}
				}
			}
			if(grandchild.size() > 0) {
				for(String child : grandchild) {
					for(String gp : grandparent) {
						context.write(new Text(child), new Text(gp));
					}
				}
				
			}
			System.out.println("--------------------------");
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			context.write(new Text("grandchild"), new Text("grandparent"));
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Path outputPath = new Path("hdfs://192.168.0.117:9000/user/root/output/singleTable");
		
//		jobRun();
		getFile(outputPath);
	}
	
	public static void jobRun()throws Exception  {
		Path outputPath = new Path("hdfs://192.168.0.117:9000/user/root/output/singleTable");
		Path inputpath = new Path("hdfs://192.168.0.117:9000/user/root/test/singleTable.txt");
		Configuration conf = new Configuration();
		 FileSystem hdfs = FileSystem.get(conf);
		boolean hasFile = hdfs.exists(outputPath);
		if(hasFile) {
			hdfs.delete(outputPath, true);
		}
		Job job = Job.getInstance();
		job.setJarByClass(SingleTable.class);
		job.setMapperClass(MyMap.class);
		job.setCombinerClass(Reducer.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputPath);
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
