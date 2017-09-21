package com.faceye.component.data.spark.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.conf.Configuration;
import com.faceye.component.data.kafka.DefaultProducer;
import com.faceye.component.data.kafka.KafkaConstants;
import com.faceye.component.data.spark.RunTemplate;
import com.faceye.component.data.spark.SparkConstants;
import com.faceye.component.data.spark.stream.domain.RealIDCheckRecord;
import com.faceye.component.data.spark.stream.domain.StatCompany;
import com.faceye.component.data.spark.stream.domain.StatRecord;
import com.faceye.component.data.spark.stream.output.StreamingOutput;
import com.faceye.component.data.util.JsonUtil;

import scala.Tuple2;

public class KafkaStreaming extends RunTemplate {
	private Logger logger = LoggerFactory.getLogger(KafkaStreaming.class);
	Map<String, Object> kafkaParams = null;
	Collection<String> topics = null;

	public KafkaStreaming() {
		System.out.println(">>Init kafka stream conf.");
		kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", Configuration.get("kafka.broker.list"));
		kafkaParams.put("key.deserializer", Configuration.get("kafka.key.deserializer"));
		kafkaParams.put("value.deserializer", Configuration.get("kafka.value.deserializer"));
		// kafkaParams.put("group.id", "test");
		kafkaParams.put("group.id", "default-msg");
		// kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);
		topics = Arrays.asList("topic-of-express");
		// topics = Arrays.asList("default-msg");
	}

	protected void exec() {
		SparkConf conf = new SparkConf().setMaster(Configuration.get("spark.master")).setAppName("kafka-stream");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		JavaPairDStream<String, RealIDCheckRecord> lines = stream.mapToPair(s -> {
			String json = s.value();
			RealIDCheckRecord record = JsonUtil.toObject(json, RealIDCheckRecord.class);
			return new Tuple2(record.getExpressOrgCode(), record);
		});

		/**
		 * 按企业维度统计
		 */
		JavaPairDStream<StatCompany, Integer> companyCounts = lines.mapToPair(s -> {
			String key = s._1;
			RealIDCheckRecord record = s._2;
			StatCompany sc = new StatCompany();
			sc.setCheckDate(StringUtils.substring(record.getCheckDate(), 0, 8));
			sc.setExpOrgCode(record.getExpressOrgCode());
			sc.setIsReported(record.getIsReported());
			return new Tuple2(sc, 1);
		});

		JavaPairDStream<StatCompany, Integer> countByCompany = companyCounts.reduceByKey((s1, s2) -> s1 + s2);
		countByCompany.foreachRDD(rdd -> {
			rdd.foreachPartition(records -> {
				List<StatCompany> scs = new ArrayList<StatCompany>(0);
				records.forEachRemaining(tup -> {
					StatCompany sc = tup._1;
					Integer total = tup._2;
					sc.setTotal(total);
					scs.add(sc);
				});
				List<String> msgs = new ArrayList<>();
				for (StatCompany sc : scs) {
					msgs.add(JsonUtil.toJson(sc));
				}
				DefaultProducer producer = new DefaultProducer();
				producer.send(KafkaConstants.TOPIC_STAT_COMPANY, msgs);
				// StreamingOutput.outputStreamResultOfCompany(scs);
			});
		});

		JavaPairDStream<StatRecord, Integer> counts = lines.mapToPair(s -> {
			String key = s._1;
			RealIDCheckRecord record = s._2;
			StatRecord statRecord = new StatRecord();
			statRecord.setCheckDate(StringUtils.substring(record.getCheckDate(), 0, 8));
			statRecord.setCity(record.getSenderAddress().getCityCode());
			statRecord.setProvince(record.getSenderAddress().getProvinceCode());
			statRecord.setCountry(record.getSenderAddress().getCountyCode());
			statRecord.setExpOrgCode(record.getExpressOrgCode());
			statRecord.setCheckMethod(record.getCheckMethod());
			statRecord.setIsReported(record.getIsReported());
			return new Tuple2(statRecord, 1);
		});

		/**
		 * 完全实时统计，所有统计维度
		 */
		JavaPairDStream<StatRecord, Integer> coutByExpOrgCode = counts.reduceByKey((s1, s2) -> s1 + s2);
		coutByExpOrgCode.foreachRDD(rdd -> {
			rdd.foreachPartition(records -> {
				List<StatRecord> statRecords = new ArrayList<>(0);
				records.forEachRemaining(tup -> {
					StatRecord sr = tup._1;
					Integer total = tup._2;
					sr.setTotal(total);
					statRecords.add(sr);
				});
				List<String> msgs = new ArrayList<>(0);
				for (StatRecord sr : statRecords) {
					msgs.add(JsonUtil.toJson(sr));
				}
				DefaultProducer producer = new DefaultProducer();
				producer.send(KafkaConstants.TOPIC_STAT_RECORD, msgs);
				// StreamingOutput.outputExpressDeliveryStatusStreamingResult(statRecords);
			});
		});
		try {
			streamingContext.start();
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			logger.error(">>Exception:" + e);
		}
	}

}
