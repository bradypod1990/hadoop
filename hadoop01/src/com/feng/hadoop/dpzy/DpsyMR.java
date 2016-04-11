package com.feng.hadoop.dpzy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DpsyMR {

	public static class DpsyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable v = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			if (strs != null && strs.length > 0) {
				InputSplit inputSplit = context.getInputSplit();
				if (inputSplit instanceof FileSplit) {
					FileSplit is = (FileSplit) inputSplit;
					String path = is.getPath().getName();
					for (String str : strs) {
						context.write(new Text(path + "->" + str), v);
					}
				}
			}
		}

	}

	public static class DpsyMapper2 extends
			Mapper<Text, IntWritable, Text, IntWritable> {

		private Text v = new Text("");

		@Override
		protected void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
//			String[] strs = value.toString().split("\t");
//			v.set(strs[1]);
//			context.write(new Text(strs[0]), v);
			System.out.println(key.toString() + "-------" + value.get());
			context.write(key, value);
		}
	}

	public static class DpsyReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		private Text t = new Text();
		private Text v = new Text();

		@Override
		protected void reduce(Text k, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			String[] str = k.toString().split("->");
			t.set(str[1]);
			v.set(str[0] + "->" + sum);
			context.write(t, v);
		}

	}

	public static void main(String[] args) throws Exception {
		Path outputPath = new Path(
				"hdfs://192.168.0.117:9000/user/root/output/dpsy");
		Path inputpath = new Path(
				"hdfs://192.168.0.117:9000/user/root/test/dpsy/");
		Configuration conf = new Configuration();
		// FileSystem hdfs = FileSystem.get(conf);
		// boolean hasFile = hdfs.exists(outputPath);
		// if (hasFile) {
		// hdfs.delete(outputPath, true);
		// }
		// Job job = Job.getInstance();
		//
		// job.setJarByClass(DpsyMR.class);
		//
		// job.setMapperClass(DpsyMapper.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		// job.setReducerClass(DpsyReducer.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		//
		// FileInputFormat.setInputPaths(job, inputpath);
		// FileOutputFormat.setOutputPath(job, outputPath);
		//
		// job.waitForCompletion(true);

//		getFile(outputPath);
		
		
		FileSystem hdfs = FileSystem.get(conf);
		 boolean hasFile = hdfs.exists(outputPath);
		 if (hasFile) {
		 hdfs.delete(outputPath, true);
		 }
		 Job job = Job.getInstance();
		
		 job.setJarByClass(DpsyMR.class);
		 ChainMapper.addMapper(job, DpsyMapper.class, IntWritable.class, Text.class, Text.class, IntWritable.class, new Configuration(false));
		 ChainMapper.addMapper(job, DpsyMapper2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));
		 
		 ChainReducer.setReducer(job, DpsyReducer.class, Text.class, IntWritable.class, Text.class, Text.class, new Configuration(false));
//		 job.setMapperClass(DpsyMapper.class);
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
//		 job.setReducerClass(DpsyReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		
		 FileInputFormat.setInputPaths(job, inputpath);
		 FileOutputFormat.setOutputPath(job, outputPath);
		
		 job.waitForCompletion(true);
	}

	public static void getFile(Path path) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(path)) {
			FileStatus[] stats = fs.listStatus(path);
			for (FileStatus file : stats) {
				FSDataInputStream is = fs.open(file.getPath());
				// get the file info to create the buffer
				FileStatus stat = fs.getFileStatus(file.getPath());
				// System.out.println(stat.getLen() + "----" + stat.getOwner());
				// create the buffer
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat
						.getLen()))];
				is.readFully(0, buffer);
				String content = new String(buffer, "UTF-8");
				System.out.println(content);
				is.close();

			}
			fs.close();
		}
	}
}
