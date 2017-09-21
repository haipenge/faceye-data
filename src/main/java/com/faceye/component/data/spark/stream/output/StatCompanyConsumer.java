package com.faceye.component.data.spark.stream.output;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.faceye.component.data.kafka.AbstractConsumer;
import com.faceye.component.data.kafka.KafkaConstants;
import com.faceye.component.data.spark.stream.domain.StatCompany;
import com.faceye.component.data.util.JsonUtil;

/**
 * 公司分析消费类
 * @author songhaipeng
 *
 */
public class StatCompanyConsumer extends AbstractConsumer {

	@Override
	public void consumer() {
		Consumer consumer = getConsumer();
		if (consumer != null) {
			consumer.subscribe(Arrays.asList(KafkaConstants.TOPIC_STAT_COMPANY));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				if (records != null && !records.isEmpty()) {
					for (ConsumerRecord<String, String> record : records) {
						String value = record.value();
						if (StringUtils.isNotBlank(value)) {
							System.out.println("V:"+value);
							StatCompany sc = JsonUtil.toObject(value, StatCompany.class);
							StreamingOutput.outputStreamResultOfCompany(Arrays.asList(sc));
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
		StatCompanyConsumer consumer=new StatCompanyConsumer();
		consumer.consumer();
	}

}
