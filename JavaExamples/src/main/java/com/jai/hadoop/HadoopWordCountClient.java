package com.jai.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopWordCountClient {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Path path = new Path("./input");
		Path outputDir = new Path("./output");
		
		JobConf conf = new JobConf(true);
		conf.setWorkingDirectory(outputDir);
		Job job = Job.getInstance(conf,"WC");

		job.setJarByClass(HadoopWordCountClient.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMaxReduceAttempts(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(conf, path);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(conf, outputDir);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		
		org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(conf);
		if(hdfs.exists(path)){
			hdfs.delete(outputDir,true);
		}

		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}
		// job.setInputFormatClass(TextInputFormat.class);
	}

	public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String v = value.toString();
			context.write(new Text(v), new IntWritable(1));
		}

	}

	public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context ctx) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			ctx.write(key, new IntWritable(sum));

		}

	}

}
