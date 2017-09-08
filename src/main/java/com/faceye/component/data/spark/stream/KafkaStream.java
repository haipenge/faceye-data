package com.faceye.component.data.spark.stream;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.faceye.component.data.spark.RunTemplate;
import com.faceye.component.data.spark.SparkConstants;

import scala.Tuple2;

public class KafkaStream extends RunTemplate {
	Map<String, Object> kafkaParams = null;
	Collection<String> topics = null;

	public KafkaStream() {
		System.out.println(">>Init kafka stream conf.");
		kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "10.1.4.236:9092,10.1.4.237:9092,10.1.4.240:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		// kafkaParams.put("group.id", "test");
		kafkaParams.put("group.id", "default-msg");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		// topics = Arrays.asList("topic-of-express");
		topics = Arrays.asList("default-msg");
	}

	@Override
	protected void exec() {
		System.out.println(">>Exec kafka message consumer.");
		SparkConf conf = new SparkConf().setMaster(SparkConstants.MASTER).setAppName("kafka-stream");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		// stream.mapToPair(record -> {
		// System.out.println(">> Key
		// is:"+record.key()==null?"Empty":record.key());
		// System.out.println(">>Value is:"+record.value());
		// return new Tuple2<>(record.key(), record.value());
		// });
		// stream.maptop
		// JavaPairDStream<String, String> pairs = stream.mapToPair(record ->
		// new Tuple2<>(record.key(), record.value()));
		// pairs.print();
		// stream.redu
		JavaDStream<String> words = stream.flatMap(record -> {
			System.out.println(">>>>>>>>>>>>>Do Split messages now.");
			return Arrays.asList(record.value().split(" ")).iterator();
		});
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		JavaPairDStream<String, Integer> counts = pairs.reduceByKey((s1, s2) -> s1 + s2);
		counts.foreachRDD(rdd -> {
			System.out.println(">>>>>>>>>>>>>>Run For each records .....");
			List<Tuple2<String, Integer>> res = rdd.collect();
			System.out.println(">>>>>>>res size is:" + res.size());
			for(Tuple2<String,Integer> tup:res){
				String key = tup._1;
				Integer count = tup._2;
				System.out.println(">>++++>--------Key is:" + key + ",Count is:" + count);
			}
			rdd.foreach(tup -> {
				System.out.println("-----------------------------------------------");
				String key = tup._1;
				Integer count = tup._2;
				System.out.println(">>>--------Key is:" + key + ",Count is:" + count);
			});
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<End Run For each records .....");
//			rdd.foreachPartition(records -> {
//				while (records.hasNext()) {
//					Tuple2<String, Integer> tup = records.next();
//					String key = tup._1;
//					Integer count = tup._2;
//					System.out.println(">>>--------Key is:" + key + ",Count is:" + count);
//				}
//				records.forEachRemaining(tup -> {
//					String key = tup._1;
//					Integer count = tup._2;
//					System.out.println(">>>Key is:" + key + ",Count is:" + count);
//				});
//				// while (pair.hasNext()) {
//				// Tuple2<String, Integer> tup = pair.next();
//				// String key = tup._1;
//				// Integer count = tup._2;
//				// System.out.println(">>>Key is:"+key+",Count is:"+count);
//				// }
//			});
		});
		counts.print();

		// stream.ma

		// OffsetRange[] offsetRanges = {
		// // topic, partition, inclusive starting offset, exclusive ending
		// // offset
		// OffsetRange.create("test", 0, 0, 100), OffsetRange.create("test", 1,
		// 0, 100) };

		// JavaRDD<ConsumerRecord<String, String>> rdd =
		// KafkaUtils.createRDD(sc, kafkaParams, offsetRanges,
		// LocationStrategies.PreferConsistent());

		// stream.foreachRDD(rdd -> {
		// OffsetRange[] offsetRanges = ((HasOffsetRanges)
		// rdd.rdd()).offsetRanges();
		// logger.debug(">>Array size is:" + (offsetRanges == null ? 0 :
		// offsetRanges.length));
		// rdd.foreachPartition(consumerRecords -> {
		// OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
		// StringBuilder sb = new StringBuilder();
		// sb.append("----------------------------------------Start---------------------------");
		// sb.append("\n");
		// sb.append("Topic:" + o.topic());
		// sb.append("\n");
		// sb.append("Partition:" + o.partition());
		// sb.append("\n");
		// sb.append("FromOffset:" + o.fromOffset());
		// sb.append("\n");
		// sb.append("UntilOffset:" + o.untilOffset());
		// sb.append("\n");
		// sb.append(o.toString());
		// sb.append("\n");
		// sb.append("-----------------------------------------END----------------------------");
		// System.out.println(o.topic() + ":" + o.partition() + ":" +
		// o.fromOffset() + ":" + o.untilOffset());
		// System.out.println(sb.toString());
		// logger.debug(">>Msg is:" + sb.toString());
		// });
		// });
		try {
			streamingContext.start();
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			logger.error(">>Exception:" + e);
		}
	}

}
