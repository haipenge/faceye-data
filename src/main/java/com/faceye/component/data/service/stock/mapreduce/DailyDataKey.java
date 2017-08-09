package com.faceye.component.data.service.stock.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DailyDataKey implements WritableComparable<DailyDataKey> {
	// 股票ID
	private LongWritable stockId = null;
	// 数据日期
	private Text date = null;

	public DailyDataKey() {
		stockId = new LongWritable();
		date = new Text();
	}

	public LongWritable getStockId() {
		return stockId;
	}

	public void setStockId(LongWritable stockId) {
		this.stockId = stockId;
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.stockId.write(out);
		this.date.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stockId.readFields(in);
		this.date.readFields(in);
	}

	@Override
	public int compareTo(DailyDataKey o) {
		int res = 0;
		res = o.getStockId().compareTo(this.getStockId());
		if (res == 0) {
			res = o.date.compareTo(this.getDate());
		}
		return res;
		// return this.stockId.compareTo(o.getStockId());
	}

	public int hashCode() {
		return this.stockId.hashCode() * 157 + this.date.hashCode();
	}

	public boolean equals(Object right) {
		if (right == null) {
			return false;
		}
		if (this == right) {
			return true;
		}
		if (right instanceof DailyDataKey) {
			DailyDataKey o = (DailyDataKey) right;
			return o.getStockId().equals(this.getStockId()) && o.getDate().equals(this.getDate());
		} else {
			return false;
		}
	}
}
