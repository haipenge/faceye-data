package com.faceye.component.data.mysql.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.Runner;
import com.faceye.component.data.mysql.io.StockRecord;
import com.faceye.component.data.mysql.map.StockMapper;

//extends Configured implements Tool
public class Stock2HDFS {
	private Logger logger = LoggerFactory.getLogger(Runner.class);

	public static void main(String[] args) throws Exception {
		// ToolRunner.run(new Stock2HDFS(), args);
//		Job job=Job.getInstance();
		JobConf conf = new JobConf(Stock2HDFS.class);
		// DistributedCache.addFileToClassPath(new Path("/lib/mysql-connector-java-6.0.6-bin.jar"), conf);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/faceye_blog", "root", "root");
		String[] fields = { "id", "name", "code", "stock_trade_id" };
		DBInputFormat.setInput(conf, StockRecord.class, "analyzer_stock", null, "id", fields);
		Path path = new Path("/mysql/stock");
		FileOutputFormat.setOutputPath(conf, path);

		conf.setInputFormat(DBInputFormat.class);
//		conf.setMapperClass(StockMapper.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setReducerClass(IdentityReducer.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);
		JobClient.runJob(conf);
//		return 0;
	}

	public Stock2HDFS() {

	}

	// @Override
	// public int run(String[] arg0) {
	//
	//
	// }
}
