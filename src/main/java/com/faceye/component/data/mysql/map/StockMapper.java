package com.faceye.component.data.mysql.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.mysql.io.StockRecord;

public class StockMapper extends Mapper<LongWritable, StockRecord, LongWritable, Text> {

	private Logger logger = LoggerFactory.getLogger(StockMapper.class);
	// @Override
	// public void map(LongWritable arg0, StockRecord arg1, OutputCollector<LongWritable, Text> arg2, Reporter arg3) throws IOException {
	// logger.debug(">>Record is:"+arg1.toString());
	// arg2.collect(new LongWritable(Long.parseLong(arg1.getId())), new Text(arg1.toString()));
	// }

	protected void map(LongWritable key, StockRecord arg1, org.apache.hadoop.mapreduce.Mapper<LongWritable, StockRecord, LongWritable, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		context.write(new LongWritable(Long.parseLong(arg1.getId())), new Text(arg1.toString()));
//		context.write(new Text(value.toString()), new IntWritable(1));
	}

}
