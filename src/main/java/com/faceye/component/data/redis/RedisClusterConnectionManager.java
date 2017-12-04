package com.faceye.component.data.redis;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.conf.Configuration;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Pool;

/**
 * Redis集群连接池
 * 
 * @author songhaipeng
 *
 */
public class RedisClusterConnectionManager extends AbstractConnectionManager {
	private Logger logger = LoggerFactory.getLogger(RedisClient.class);

	// exam-> host:port,host:port....
	private static String HOST = Configuration.get("redis.cluster");

	private static Set<HostAndPort> NODES = null;

	private static JedisCluster JEDIS_CLUSTER = null;

	private RedisClusterConnectionManager() {
		init();
	}

	private void init() {
		if (NODES == null) {
			NODES = new HashSet<HostAndPort>();
			String[] serverArray = StringUtils.split(HOST, ":");
			for (String ipPort : serverArray) {
				String[] ipPortPair = ipPort.split(":");
				NODES.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
			}
		}
		if (JEDIS_CLUSTER == null) {
			JEDIS_CLUSTER = new JedisCluster(NODES, 30000, this.getJedisPoolConfig());
		}
	}

	/**
	 * 获取Reids集群连接池
	 */

	private static class RedisClusterConnectionManagerHolder {
		private static RedisClusterConnectionManager INSTANCE = new RedisClusterConnectionManager();
	}

	public static RedisClusterConnectionManager getInstance() {
		return RedisClusterConnectionManagerHolder.INSTANCE;
	}

	public JedisCluster getJedis() {

		return JEDIS_CLUSTER;
	}

	public void close() {
	}

	public void set(String key, long seconds, Object value) {
		JedisCluster jedis = JEDIS_CLUSTER;
		try {
			if (jedis != null) {
				jedis.set(key.getBytes(), ObjectTranscoder.serialize(value), "NX".getBytes(), "EX".getBytes(), seconds);
			} else {
				logger.error(">>Jedis is null of key :" + key);
			}
		} catch (Exception e) {
			logger.error(">>Ex:", e);
		} finally {
			close();
		}
	}

	public Object get(String key) {
		JedisCluster jedis = JEDIS_CLUSTER;
		Object obj = null;
		try {
			if (jedis != null && jedis.exists(key.getBytes())) {
				byte[] in = jedis.get(key.getBytes());
				obj = ObjectTranscoder.deserialize(in);
			}
		} catch (Exception e) {
			logger.error(">>E:", e);
		} finally {
			close();
		}
		return obj;
	}

}
