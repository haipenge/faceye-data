package com.faceye.component.data.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.conf.Configuration;

public abstract class AbstractConsumer {
  protected Logger logger=LoggerFactory.getLogger(getClass());
  
  protected Consumer getConsumer(){
	  Consumer consumer=null;
	  Map props=new HashMap();
	//brokerServer(kafka)ip地址,不需要把所有集群中的地址都写上，可是一个或一部分
	  props.put("bootstrap.servers", Configuration.get("kafka.broker.list"));
	  //设置consumer group name,必须设置
	  props.put("group.id", "default-group");
	  //设置自动提交偏移量(offset),由auto.commit.interval.ms控制提交频率
	  props.put("enable.auto.commit", "true");
	  //偏移量(offset)提交频率
	  props.put("auto.commit.interval.ms", "1000");
	  //设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
	  //如果采用latest，消费者只能得道其启动后，生产者生产的消息
	  props.put("auto.offset.reset", "earliest");
	  //设置心跳时间
	  props.put("session.timeout.ms", "30000");
	  //设置key以及value的解析（反序列）类
	  props.put("key.deserializer", Configuration.get("kafka.key.deserializer"));
	  props.put("value.deserializer",Configuration.get("kafka.value.deserializer"));
	  consumer =new KafkaConsumer(props);
	  return consumer;
  }
  
  public abstract void consumer();
}
