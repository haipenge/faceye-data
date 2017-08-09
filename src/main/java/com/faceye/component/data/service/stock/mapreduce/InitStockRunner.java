package com.faceye.component.data.service.stock.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

public class InitStockRunner extends MongoTool {
	private Logger logger = LoggerFactory.getLogger(InitStockRunner.class);
	
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new InitStockRunner(), args);
	}
	
	public InitStockRunner(){
		Configuration conf = new Configuration();
		conf.addResource("mongo-defaults.xml");
		logger.debug(">>FaceYe is getConf() is null?"+(null==getConf()));
		setConf(conf);
		logger.debug(">>FaceYe isMapReduced v1?"+MongoTool.isMapRedV1());
		MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
		MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
//		MongoConfig config=new MongoConfig(getConf());
		
		logger.debug(">>FaceYe is getConf() is null?"+(null==getConf()));
		logger.debug(">>FaceYe -->"+getConf().get("dfs.namenode.name.dir"));
		logger.debug(">>FaceYe --> mongo.input.uri :"+getConf().get("mongo.input.uri"));
//		config.setInputFormat(MongoInputFormat.class);
		
		MongoConfigUtil.setMapper(getConf(), InitStockMapper.class);
		//MongoConfigUtil.setCombiner(getConf(), InitStockReducer.class);
		MongoConfigUtil.setReducer(getConf(), InitStockReducer.class);
		
		
		MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
		MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);
		
		
//		MongoConfigUtil
//		config.setMapper(InitStockMapper.class);
//		config.setMapperOutputKey(Text.class);
//		config.setMapperOutputValue(IntWritable.class);
		
		MongoConfigUtil.setOutputKey(getConf(), LongWritable.class);
		MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);
		
		
		
		MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost/search.stock_stock");
		MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost/search.stock_category");
		
//		config.setOutputKey(Text.class);
//		config.setOutputValue(IntWritable.class);
//		config.setReducer(InitStockReducer.class);
//		config.setInputURI("mongodb://localhost/search.stock_stock");
//		config.setOutputURI("mongodb://localhost/search.stock_stock_analyzer");
		
	}
	
	public void init(){
		
		
	}
//	@Override
//	public int run(String[] args) throws Exception {
//		Configuration conf = getConf();
////	
//		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/search.stock_stock");
//		MongoConfigUtil.setOutputURI(conf,"mongodb://localhost/search.stock_stock_analyzer");
//		 JobConf jobConf = new JobConf(conf);
////		 jobConf.setMapperClass(InitStockMapper.class);
////		 jobConf.setReducerClass(InitStockReducer.class);
////		 jobConf.setOutputFormat(MongoOutputFormat.class);
////		 jobConf.setOutputKeyClass(MongoConfigUtil.getOutputKey(conf));
//		 jobConf.setOutputKeyClass(Text.class);
////		 jobConf.setOutputValueClass(MongoConfigUtil.getOutputValue(conf));
//		 jobConf.setOutputValueClass(BSONWritable.class);
////		 jobConf.setMapOutputKeyClass(MongoConfigUtil.getMapperOutputKey(conf));
//		 jobConf.setMapOutputKeyClass(Text.class);
////		 jobConf.setMapOutputValueClass(MongoConfigUtil.getMapperOutputValue(conf));
//		 jobConf.setMapOutputValueClass(IntWritable.class);
////		 jobConf.setInputFormat(MongoInputFormat.class);
////		 job.addResource(file);
//		 Job job=Job.getInstance(jobConf);
//		 job.setMapperClass(InitStockMapper.class);
//		 job.setReducerClass(InitStockReducer.class);
//		 job.setMapOutputKeyClass(MongoOutputFormat.class);
//		 job.setInputFormatClass(MongoInputFormat.class);
//		
////		 Path path=new Path(conf.get("faceye.third.party.lib"));
////		 job.addArchiveToClassPath(path);
////		 FileSystem fs = FileSystem.getLocal(conf);
////		 String newJarPath = new Path(jarPath).makeQualified(fs).toString();
////		 String tmpjars = conf.get("tmpjars");
////		 if (tmpjars == null || tmpjars.length() == 0) {
////		 conf.set("tmpjars", newJarPath);
////		 } else {
////		 conf.set("tmpjars", tmpjars + "," + newJarPath);
////		 }
////		 }
////		 logger.debug(">>FaceYe -->lib path is :"+path.toString());
//		 job.submit();
//		 System.exit( job.waitForCompletion( true ) ? 0 : 1 ); 
////		 job.setOutputFormatClass(MongoOutputFormat.class);
//		
////		 JobClient.runJob(job);
//		
//		// JobClient.runJob(j.g)
//		return 0;
//	}

}
