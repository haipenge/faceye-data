package com.faceye.component.data.spark.stream.output;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.faceye.component.data.redis.MessageConsumer;
import com.faceye.component.data.spark.SparkConstants;
import com.faceye.component.data.spark.stream.domain.StatCompany;
import com.faceye.component.data.util.JsonUtil;
/**
 * 全量统计topic消费类
 * @author songhaipeng
 *
 */
public class ResultConsumer extends MessageConsumer {

	public ResultConsumer(String topic) {
		super(topic);
	}

	@Override
	public void exec(String msg) {
		if (StringUtils.equals(topic, SparkConstants.TOPIC_STAT_COMPANY)) {
			statCompany(msg);
		}
	}

	private void statCompany(String msg) {
		if (StringUtils.isNotEmpty(msg)) {
			StatCompany sc = JsonUtil.toObject(msg, StatCompany.class);
			List<StatCompany> results = new ArrayList<>(0);
			results.add(sc);
			StreamingOutput.outputStreamResultOfCompany(results);
		}
	}

	public static void main(String[] args) {
		ResultConsumer consumer = new ResultConsumer(SparkConstants.TOPIC_STAT_COMPANY);
		consumer.consumer();
	}

}
