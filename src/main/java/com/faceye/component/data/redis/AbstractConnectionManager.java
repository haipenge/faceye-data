package com.faceye.component.data.redis;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis连接池抽像类
 * @author songhaipeng
 *
 */
public abstract class AbstractConnectionManager {
	protected JedisPoolConfig getJedisPoolConfig() {
		int maxActive = 1024;
		int maxIdle = 1024;
		int maxWait = 30000;
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxActive);// 老版本是setMaxActive
		config.setMaxIdle(maxIdle);
		config.setMaxWaitMillis(maxWait);// 老版本是maxMaxWait
		config.setTestOnBorrow(true);
		return config;
	}
	
	abstract public JedisCommands getJedis();
	
	abstract public void close();
	
}
