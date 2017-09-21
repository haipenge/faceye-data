package com.faceye.component.data.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;

import kafka.serializer.StringEncoder;

public class KafkaConstants {
//	public static final String BROKER_LIST="10.1.4.236:9092,10.1.4.237:9092,10.1.4.240:9092";
////	public static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类  
//	public static final Class SERIALIZER_CLASS = StringEncoder.class;
//	public static final Class KEY_DESERIALIZER=StringDeserializer.class;
//	public static final Class VALUE_DESERIALIZER=StringDeserializer.class;
	
	
	//TOPICS
	//全量分析topic
	public static final String TOPIC_STAT_RECORD="topic-stat-record";
	//按公司维度分析topic
	public static final String TOPIC_STAT_COMPANY="topic-stat-company";
	
	
	
}
