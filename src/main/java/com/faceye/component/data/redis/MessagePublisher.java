package com.faceye.component.data.redis;

import redis.clients.jedis.Jedis;

public class MessagePublisher {

	public static void publish(String channel,String msg){
		Jedis jedis=RedisConnectionManager.getInstance().getJedis();
		jedis.lpush(channel, msg);
		RedisConnectionManager.getInstance().close();
	}
}
