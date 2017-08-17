package com.faceye.component.data.hbase.job.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.faceye.component.data.hbase.job.map.ExampleMapper;
import com.faceye.component.data.hbase.job.reduce.ExampleReducer;


public class ExampleRunner extends Configured implements Tool {

	// public void test() {
	// Configuration config = HBaseConfiguration.create();
	// Job job = new Job(config, "ExampleReadWrite");
	// job.setJarByClass(MyReadWriteJob.class); // class that contains mapper
	//
	// Scan scan = new Scan();
	// scan.setCaching(500); // 1 is the default in Scan, which will be bad for
	// // MapReduce jobs
	// scan.setCacheBlocks(false); // don't set to true for MR jobs
	// // set other scan attrs
	//
	// TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
	// scan, // Scan instance to control CF and attribute selection
	// ExampleMapper.class, // mapper class
	// null, // mapper output key
	// null, // mapper output value
	// job);
	// TableMapReduceUtil.initTableReducerJob(targetTable, // output table
	// null, // reducer class
	// job);
	// job.setNumReduceTasks(0);
	//
	// boolean b = job.waitForCompletion(true);
	// if (!b) {
	// throw new IOException("error with job!");
	// }
	// }

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "ExampleReadWrite");
		job.setJarByClass(ExampleReducer.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs
		TableMapReduceUtil.initTableMapperJob("example", // input table
				scan, // Scan instance to control CF and attribute selection
				ExampleMapper.class, // mapper class
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("summary_example", // output
																	// table
				ExampleReducer.class, // reducer class
				job);
		job.setNumReduceTasks(1);
//		job.setOutputFormatClass(MultiTableOutputFormat.class);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExampleRunner(),args);
	}
}
