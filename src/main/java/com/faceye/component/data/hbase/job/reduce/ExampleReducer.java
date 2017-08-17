package com.faceye.component.data.hbase.job.reduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ExampleReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	public static final byte[] CF = "cf_".getBytes();
	public static final byte[] COUNT = "_age_count".getBytes();
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(CF, COUNT, Bytes.toBytes(i));
		context.write(null, put);
	}
}
