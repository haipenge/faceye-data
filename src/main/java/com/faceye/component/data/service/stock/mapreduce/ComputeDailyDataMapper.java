package com.faceye.component.data.service.stock.mapreduce;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class ComputeDailyDataMapper extends Mapper<Number, BSONObject, DailyDataKey, BSONWritable> {
	private Log logger = LogFactory.getLog(getClass());

	protected void map(Number key, BSONObject value, Context context) throws IOException, InterruptedException {
		logger.debug(">>FaceYe Start to run ComputeDailyDataMapper now.");
		Long stockId = null;
		if (value.containsField("stockId")) {
			String stockIdFieldValue = value.get("stockId").toString();
			Date date=(Date) value.get("date");
			if (StringUtils.isNotEmpty(stockIdFieldValue)) {
				stockId = Long.parseLong(stockIdFieldValue);
				// logger.info(">>FaceYe -->Map --> stock id:" + stockId + ",dailyData id:" + value.get("_id"));
				DailyDataKey dailyDataKey=new DailyDataKey();
				dailyDataKey.setStockId(new LongWritable(stockId));
				dailyDataKey.setDate(new Text(""+date.getTime()));
				context.write(dailyDataKey, new BSONWritable(value));
			}
		}
	}
}
