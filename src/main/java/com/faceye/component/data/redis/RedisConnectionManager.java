package com.faceye.component.data.redis;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.conf.Configuration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Pool;

/**
 * Redis连接池
 * @author songhaipeng
 *
 */
public class RedisConnectionManager extends AbstractConnectionManager {

	private Logger logger = LoggerFactory.getLogger(RedisClient.class);
	private static Pool<Jedis> JEDIS_POOL = null;

	private static Pool<ShardedJedis> SHARD_JEDIS_POOL = null;
	private static String HOST = Configuration.get("redis.host");
	private static String PORT = Configuration.get("redis.port","6379");

	private static class RedisConnectionManagerHolder {
		private static RedisConnectionManager INSTANCE = new RedisConnectionManager();
	}

	public static RedisConnectionManager getInstance() {
		return RedisConnectionManagerHolder.INSTANCE;
	}

	private RedisConnectionManager() {
		initPool();
	}

	/**
	 * Jedis连接池
	 */
	private static ThreadLocal<Jedis> threadLocal = new ThreadLocal<Jedis>() {
		@Override
		protected Jedis initialValue() {
			Jedis jedis = null;
			if (JEDIS_POOL != null) {
				jedis = JEDIS_POOL.getResource();
			}
			return jedis;
		}
	};

	private void initPool() {
		this.initJedisPool();
	}

	/**
	 * 初始化单
	 */
	private void initJedisPool() {
		int port = 6379;
		int timeOut = 30000;
		if (JEDIS_POOL == null) {
			JedisPoolConfig config = getJedisPoolConfig();
			JEDIS_POOL = new JedisPool(config, HOST, port, timeOut);// 有密码的时候传入AUTH
		}
	}

	/**
	 * 是否Redis集群
	 * 
	 * @return
	 */
	private boolean isCluster() {
		return StringUtils.contains(HOST, ",");
	}

	/**
	 * 获取Redis
	 * 
	 * @return
	 */
	public Jedis getJedis() {
		return threadLocal.get();
	}

	/**
	 * 关闭连接
	 */
	public void close() {
		Jedis jedis = threadLocal.get();
		if (jedis != null) {
			jedis.close();
			threadLocal.remove();
		}
	}

	public void closePool() {
		if (JEDIS_POOL != null) {
			JEDIS_POOL.close();
		}
	}

}
