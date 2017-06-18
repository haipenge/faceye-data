package com.faceye.component.data.mysql.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.mysql.io.StockRecord;
import com.faceye.component.data.mysql.map.StockMapper;
import com.faceye.component.data.util.DataConstants;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class StockRunner extends Configured implements Tool {

	private Logger logger = LoggerFactory.getLogger(StockRunner.class);
	
	private static String TABLE="analyzer_stock";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new StockRunner(), args);
	}

	public StockRunner() {

	}

	@Override
	public int run(String[] arg0) throws Exception {
		return this.process(arg0);
	}

	private int process(String[] args) throws Exception {
//		Configuration conf=super.getConf();
		Configuration conf=new Configuration();
		Job job =Job.getInstance(conf, StockRunner.class.getName());
		job.addFileToClassPath(new Path("/lib/mysql-connector-java-5.1.42.jar"));
//		DistributedCache.addFileToClassPath(new Path("/lib/mysql-connector-java-5.1.42.jar"), conf);
		DBConfiguration.configureDB(job.getConfiguration(), DataConstants.DB_DRIVER, DataConstants.DB_URL, DataConstants.DB_USER, DataConstants.DB_PASSWORD); 
		Path outputPath=new Path(DataConstants.DB_2_HDFS_ROOT_DIR+"/"+TABLE);
		job.setInputFormatClass(DBInputFormat.class);
		String [] fieldNames=new String []{"id","name","code","stock_trade_id"};
		DBInputFormat.setInput(job, StockRecord.class, TABLE, null, "id", fieldNames);
		
		job.setJarByClass(StockRunner.class);
		job.setMapperClass(StockMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		return 0;
	}

}
