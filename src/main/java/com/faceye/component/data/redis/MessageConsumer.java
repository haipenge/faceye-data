package com.faceye.component.data.redis;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Redis消费队列
 * 
 *
 */
public abstract class MessageConsumer {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	
	public MessageConsumer(String topic){
		this.topic=topic;
	}

	protected String topic = "";

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void consumer() {
		int count = 0;
		Jedis jedis = null;
		while (true) {
			if (jedis == null) {
				jedis = RedisConnectionManager.getInstance().getJedis();
			}
			String msg = jedis.rpop(topic);
			if (StringUtils.isNotEmpty(msg)) {
				exec(msg);
			} else {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					logger.error(">>Exception:" + e);
				}
			}
			count++;
			if (count >= 50) {
				RedisConnectionManager.getInstance().close();
				jedis = null;
			}
		}
	}

	public abstract void exec(String msg);
}
