package com.faceye.component.data.hbase.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @KEY:RowKey生成器 时间(14)+服务标识(3)+自增(7)
 * @Exam: yyyyMMddHHmmss0011000000
 * @author songhaipeng
 *
 */
public class RowKeyGenerater {

	private Logger logger = LoggerFactory.getLogger(RowKeyGenerater.class);
	private SimpleDateFormat sdf = null;
	// 服务标识，用于区分不同服务器生的ID
	private static Integer SERVICE_SIGN = 001;
	private AtomicInteger COUNTER = null;
	private static Integer MAX_COUNTER = 9999999;
	private static Integer MIN_COUNTER = 1000000;

	private RowKeyGenerater() {
		sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		COUNTER = new AtomicInteger(MIN_COUNTER);
	}

	private static class RowKeyGeneraterHolder {
		private static final RowKeyGenerater INSTANCE = new RowKeyGenerater();
	}

	public static RowKeyGenerater getInstance() {
		return RowKeyGeneraterHolder.INSTANCE;
	}

	public synchronized Long get() {
		Long res = null;
		StringBuilder sb = new StringBuilder();
		String timeSign = getTimestampSign();
		sb.append(timeSign);
		sb.append(SERVICE_SIGN);
		sb.append(getCounter());
		res = Long.parseLong(sb.toString());
		return res;
	}

	/**
	 * 获取计数器
	 * 
	 * @return
	 */
	private synchronized Integer getCounter() {
		Integer counter = COUNTER.getAndIncrement();
		if (counter >= MAX_COUNTER) {
			COUNTER = new AtomicInteger(MIN_COUNTER);
		}
		return counter;
	}

	private synchronized String getTimestampSign() {
		String timeSign = sdf.format(new Date());
		return timeSign;
	}
}
