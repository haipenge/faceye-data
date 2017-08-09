package com.faceye.component.data.service.stock.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.hadoop.io.BSONWritable;



public class InitStockReducer extends Reducer<Text, IntWritable, LongWritable, BSONWritable> {
	
	private Logger logger = LoggerFactory.getLogger(InitStockReducer.class);
	private AtomicLong id=new AtomicLong(1);
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int num = 0;
		int sum = 0;
		Iterator<IntWritable> it=values.iterator();
		while(it.hasNext()){
			IntWritable count=it.next();
			sum+=count.get();
		}
		BasicBSONObject bson = new BasicBSONObject();
		logger.debug(">>FaceYe -->" + key + ":" + sum);
		bson.put("name", key.toString());
		bson.put("totalCount", sum);
		context.write(new LongWritable(id.getAndIncrement()), new BSONWritable(bson));
	}
}
