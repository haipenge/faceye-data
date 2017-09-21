package com.faceye.component.data.spark.stream.output;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.faceye.component.data.kafka.AbstractConsumer;
import com.faceye.component.data.kafka.KafkaConstants;
import com.faceye.component.data.spark.stream.domain.StatRecord;
import com.faceye.component.data.util.JsonUtil;

/**
 * 全量分析消费类
 * @author songhaipeng
 *
 */
public class StatRecordConsumer extends AbstractConsumer {

	@Override
	public void consumer() {
		Consumer consumer = getConsumer();
		if (consumer != null) {
			consumer.subscribe(Arrays.asList(KafkaConstants.TOPIC_STAT_RECORD));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				if (records != null && !records.isEmpty()) {
					for (ConsumerRecord<String, String> record : records) {
						String value = record.value();
						if (StringUtils.isNotBlank(value)) {
							System.out.println("V:"+value);
							StatRecord sr = JsonUtil.toObject(value, StatRecord.class);
							StreamingOutput.outputExpressDeliveryStatusStreamingResult(Arrays.asList(sr));
						}
					}
				} else {
					try {
						Thread.sleep(100L);
					} catch (InterruptedException e) {
						logger.error(">>Exception:" + e);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) {
		StatRecordConsumer consumer=new StatRecordConsumer();
		consumer.consumer();
	}

}
