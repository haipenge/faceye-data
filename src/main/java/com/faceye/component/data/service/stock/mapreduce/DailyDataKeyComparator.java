package com.faceye.component.data.service.stock.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DailyDataKeyComparator extends WritableComparator {
	public DailyDataKeyComparator() {
		super(DailyDataKey.class, true);
	}

	public int compare(WritableComparable combinationKeyOne, WritableComparable combinationKeyOther) {
		int res = 0;
		DailyDataKey key1 = (DailyDataKey) combinationKeyOne;
		DailyDataKey key2 = (DailyDataKey) combinationKeyOther;
		// res=key1.getDate().compareTo(key2.getDate());
		// return res;
		res = key1.getStockId().compareTo(key2.getStockId());
		if (res == 0) {
			res = key2.getDate().compareTo(key1.getDate());
		}
		return res;
	}
}
