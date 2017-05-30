package com.datafeedtoolbox.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DataFeedJob extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Only "+args.length+" parameters detected. Expected 3.");
			System.err.println("Usage: SumRevenue ColumnHeaders.tsv InHitData.tsv OutputLocation");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), DataFeedJob.class.getCanonicalName());
		job.setJarByClass(DataFeedJob.class);
		job.setMapperClass(StandardMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(StandardReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.addCacheFile(new Path(args[0]).toUri());
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DataFeedJob(), args);
		System.exit(exitCode);
	}

	public static void main_old(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Only "+args.length+" parameters detected. Expected 3.");
			System.err.println("Usage: SumRevenue ColumnHeaders.tsv InHitData.tsv OutputLocation");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "DataFeed");
		job.setJarByClass(DataFeedJob.class);
		job.setMapperClass(StandardMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(StandardReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.addCacheFile(new Path(args[0]).toUri());
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}