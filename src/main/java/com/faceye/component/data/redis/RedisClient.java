package com.faceye.component.data.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Redis客户端
 * 
 * @author songhaipeng
 *
 */
public class RedisClient {

	private Logger logger = LoggerFactory.getLogger(RedisClient.class);

	private static class RedisClientHolder {
		private static RedisClient INSTANCE = new RedisClient();
	}

	private RedisClient() {

	}

	public static RedisClient getInstance() {
		return RedisClientHolder.INSTANCE;
	}

	/**
	 * 对一般对像进行缓存
	 * 
	 * @param key
	 * @param seconds
	 * @param value
	 */
	public void set(String key, int seconds, Object value) {
		Jedis jedis = RedisConnectionManager.getInstance().getJedis();
		try {
			if (jedis != null) {
				jedis.set(key.getBytes(), ObjectTranscoder.serialize(value), "NX".getBytes(), "EX".getBytes(), seconds);
			} else {
				logger.error(">>Jedis error:jedis is null.");
			}
		} catch (Exception e) {
			logger.error("Set key error : " + e);
		} finally {
			if (jedis != null) {
				RedisConnectionManager.getInstance().close();
			}
		}
	}

	/**
	 * 获取缓存对像
	 * 
	 * @param key
	 * @return
	 */
	public Object get(String key) {
		Object obj = null;
		Jedis jedis = RedisConnectionManager.getInstance().getJedis();
		try {
			if (jedis != null && jedis.exists(key.getBytes())) {
				byte[] in = jedis.get(key.getBytes());
				obj = ObjectTranscoder.deserialize(in);
			}
		} catch (Exception e) {
			logger.error(">>Jedis error:" + e);
		} finally {
			if (jedis != null) {
				RedisConnectionManager.getInstance().close();
			}
		}
		return obj;
	}

}
