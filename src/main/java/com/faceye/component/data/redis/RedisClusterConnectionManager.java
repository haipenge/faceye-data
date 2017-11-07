package com.faceye.component.data.redis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.conf.Configuration;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Pool;

/**
 * Redis集群连接池
 * @author songhaipeng
 *
 */
public class RedisClusterConnectionManager extends AbstractConnectionManager {
	private Logger logger = LoggerFactory.getLogger(RedisClient.class);

	private static Pool<ShardedJedis> SHARD_JEDIS_POOL = null;

	// exam-> host:port,host:port....
	private static String HOST = Configuration.get("redis.cluster");

	private RedisClusterConnectionManager() {
		initShardingJedisPool();
	}

	/**
	 * 获取Reids集群连接池
	 */
	private static ThreadLocal<ShardedJedis> shardedLocal = new ThreadLocal<ShardedJedis>() {
		@Override
		protected ShardedJedis initialValue() {
			ShardedJedis jedis = null;
			if (SHARD_JEDIS_POOL != null) {
				jedis = SHARD_JEDIS_POOL.getResource();
			}
			return jedis;
		}
	};

	private static class RedisClusterConnectionManagerHolder {
		private static RedisClusterConnectionManager INSTANCE = new RedisClusterConnectionManager();
	}

	public static RedisClusterConnectionManager getInstance() {
		return RedisClusterConnectionManagerHolder.INSTANCE;
	}

	/**
	 * 初始化Redis分片连接池
	 */
	private void initShardingJedisPool() {
		ShardedJedisPool pool = null;
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		Set<HostAndPort> nodes = new HashSet<>();
		String[] serverArray = StringUtils.split(HOST, ",");
		for (String ipPort : serverArray) {
			String[] ipPortPair = ipPort.split(":");
			nodes.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
		}
		Iterator<HostAndPort> it = nodes.iterator();
		while (it.hasNext()) {
			HostAndPort host = it.next();
			JedisShardInfo shard = new JedisShardInfo(host.getHost(), host.getPort(), 30000);
			shards.add(shard);
		}
		SHARD_JEDIS_POOL = new ShardedJedisPool(getJedisPoolConfig(), shards);
	}

	public ShardedJedis getJedis() {
		return shardedLocal.get();
	}

	public void close() {
		ShardedJedis jedis = shardedLocal.get();
		if (jedis != null) {
			jedis.close();
			shardedLocal.remove();
		}

	}


}
