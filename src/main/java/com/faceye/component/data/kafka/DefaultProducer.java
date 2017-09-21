package com.faceye.component.data.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.conf.Configuration;
/**
 * 默认生产者
 * @author songhaipeng
 *
 */
public class DefaultProducer {
	protected Logger logger = LoggerFactory.getLogger(getClass());

	public DefaultProducer() {

	}

	protected Producer getProducer() {
		Producer producer = null;
		//bootstrap.servers
		Map props = new HashMap();
//		props.put("serializer.class", Configuration.get("kafka.serializer") );
		props.put("key.serializer", Configuration.get("kafka.key.serializer"));
		props.put("value.serializer", Configuration.get("kafka.value.serializer"));
//		props.put("metadata.broker.list",Configuration.get("kafka.broker.list"));
		props.put("bootstrap.servers",Configuration.get("kafka.broker.list"));
		// 是否获取反馈
		// 0是不获取反馈(消息有可能传输失败)
		// 1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
		// -1是所有in-sync replicas接受到消息时的反馈
		props.put("request.required.acks", "1");
		/**
		 * 内部发送数据是异步还是同步 sync：同步, 默认 async：异步
		 */
		// props.put("producer.type", "async");
		// 重试次数
		props.put("message.send.max.retries", "3");
		// 异步提交的时候(async)，并发提交的记录数
		// props.put("batch.num.messages", "200");
		// 设置缓冲区大小，默认10KB
		props.put("send.buffer.bytes", "102400");
		producer = new KafkaProducer(props);
		return producer;
	}

	public void send(String topic, String msg) {
		List<String> msgs = new ArrayList<String>();
		msgs.add(msg);
		send(topic, msgs);
	}

	public void send(String topic, List<String> msgs) {
		Producer producer = null;
		try {
			if (CollectionUtils.isNotEmpty(msgs)) {
				producer = getProducer();
				for (String msg : msgs) {
					ProducerRecord record = new ProducerRecord(topic, msg);
					producer.send(record);
				}
			}
		} catch (Exception e) {
			logger.error(">>FaceYe --> kafka send msg exception:" + e);
			e.printStackTrace();
		} finally {
			if (producer != null) {
				producer.close();
			}
		}
	}

}
