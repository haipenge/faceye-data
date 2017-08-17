package com.faceye.component.data.hbase.job.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 拷贝表
 * 
 * @author songhaipeng
 *
 */
public class ExampleMapper extends TableMapper<Text, IntWritable> {
	public static final byte[] CF = "other".getBytes();
	public static final byte[] ATTR1 = "age".getBytes();

	private final IntWritable ONE = new IntWritable(1);
	private Text text = new Text();

	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		String val = new String(value.getValue(CF, ATTR1));
		text.set(val); // we can only emit Writables...
		context.write(text, ONE);
	}
}
