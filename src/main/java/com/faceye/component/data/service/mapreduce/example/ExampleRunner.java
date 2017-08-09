package com.faceye.component.data.service.mapreduce.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.service.conf.BusinessConfiguration;


public class ExampleRunner implements Tool {
    private Logger logger=LoggerFactory.getLogger(ExampleRunner.class);
	@Override
	public void setConf(Configuration conf) {
	}

	@Override
	public Configuration getConf() {
		return BusinessConfiguration.create();
	}

	@Override
	public int run(String[] args) throws Exception {
		
		JobConf jobConf = new JobConf(getConf(), ExampleRunner.class);
		logger.debug(">>FaceYe --> dfs.namenode.name.dir :"+getConf().get("dfs.namenode.name.dir"));
		jobConf.setJobName("wordcount");

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		jobConf.setMapperClass(ExampleMapper.class);
		jobConf.setCombinerClass(ExampleReducer.class);
		jobConf.setReducerClass(ExampleReducer.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		
//		Job job=Job.getInstance(getConf());
//		job.setg
        
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), jobConf);
				jobConf.setBoolean("wordcount.skip.patterns", true);
			} else {
				other_args.add(args[i]);
			}
		}
		FileInputFormat.setInputPaths(jobConf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(jobConf, new Path(other_args.get(1)));
		JobClient.runJob(jobConf);
		return 0;
	}
	
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new ExampleRunner(), args);
	}

}
