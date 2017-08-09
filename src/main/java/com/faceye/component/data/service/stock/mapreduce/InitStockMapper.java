package com.faceye.component.data.service.stock.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 初始化股票数据到HDFS
 * @author @haipenge 
 * haipenge@gmail.com
*  Create Date:2015年2月25日
 */
public class InitStockMapper extends Mapper<Integer, BSONObject, Text, IntWritable>  {
	private Logger logger = LoggerFactory.getLogger(InitStockMapper.class);
	IntWritable count = new IntWritable(1);

	@Override
	public void map(Integer key, BSONObject value, Context context) throws IOException, InterruptedException {
		String business = value.get("business").toString();
		String stockName = value.get("name").toString();
		logger.debug(">>FaceYe:" + key + ":" + business + "," + stockName);
		Text outputKey = new Text(business);
		Object stockId = value.get("stockId");
		try {
			ToolRunner.run(new ComputeDailyDataRunner(new Long(stockId.toString())), null);
		} catch (NumberFormatException e) {
			logger.error(">>FaceYe throws Exception: --->",e);
		} catch (Exception e) {
			logger.error(">>FaceYe throws Exception: --->",e);
		}
		// output.collect(outputKey, count);
		context.write(outputKey, count);
	}

}
