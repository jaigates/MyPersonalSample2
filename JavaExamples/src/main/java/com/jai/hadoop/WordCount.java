package com.jai.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text outputKey;
	private IntWritable outputVal;

	@Override
	public void setup(Context context) {
		outputKey = new Text();
		outputVal = new IntWritable(1);
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer stk = new StringTokenizer(value.toString());
		while (stk.hasMoreTokens()) {
			outputKey.set(stk.nextToken());
			context.write(outputKey, outputVal);
		}
	}
}

class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result;

	@Override
	public void setup(Context context) {
		result = new IntWritable();
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}

public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String in;
		String out;

		if (args.length != 2) {
			in = "./input";
			out = "./output";
		} else {
			in = args[0];
			out = args[1];
		}
		Job job = Job.getInstance(conf, "Word Count");

		// set jar
		job.setJarByClass(WordCount.class);

		// set Mapper, Combiner, Reducer
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		/*
		 * Optional, set customer defined Partioner:
		 * job.setPartitionerClass(MyPartioner.class);
		 */

		// set output key
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set input and output path

		FileInputFormat.addInputPath(job, new Path(in));

		FileOutputFormat.setOutputPath(job, new Path(out));

		// by default, Hadoop use TextInputFormat and TextOutputFormat
		// any customer defined input and output class must implement
		// InputFormat/OutputFormat interface
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}