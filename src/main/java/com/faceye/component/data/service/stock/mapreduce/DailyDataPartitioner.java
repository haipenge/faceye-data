package com.faceye.component.data.service.stock.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

import com.mongodb.hadoop.io.BSONWritable;

public class DailyDataPartitioner extends Partitioner<DailyDataKey, BSONWritable> {

	@Override
	public int getPartition(DailyDataKey key, BSONWritable value, int numPartitions) {
		// return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numPartitions;
		return (key.getStockId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
