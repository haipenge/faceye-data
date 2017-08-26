package com.faceye.component.data.storm.example;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka+Storm版  work count
 * 
 * @author haipenge
 *
 */
public class KafkaWordCount {
	private static Logger logger = LoggerFactory.getLogger(KafkaWordCount.class);

	public static class KafkaWordSpliter extends BaseRichBolt {
		private OutputCollector collector;

		@Override
		public void execute(Tuple arg0) {
			String line = arg0.getString(0);
			String[] words = line.split("\\s+");
			for (String word : words) {
				logger.debug("EMIT[splitter -> counter] " + word);
				collector.emit(arg0, new Values(word, 1));
			}
			collector.ack(arg0);

		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			this.collector = arg2;

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			arg0.declare(new Fields("word", "count"));
		}
	}

	public static class WordCounter extends BaseRichBolt {
		private OutputCollector collector;
		private Map<String, AtomicInteger> counterMap;

		@Override
		public void execute(Tuple input) {
			String word = input.getString(0);
			int count = input.getInteger(1);
			logger.debug("RECV[splitter -> counter] " + word + " : " + count);
			AtomicInteger ai = this.counterMap.get(word);
			if (ai == null) {
				ai = new AtomicInteger();
				this.counterMap.put(word, ai);
			}
			ai.addAndGet(count);
			collector.ack(input);
			logger.debug("CHECK statistics map: " + this.counterMap);
		}

		@Override
		public void cleanup() {
			logger.info("The final result:");
			Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, AtomicInteger> entry = iter.next();
				logger.info(entry.getKey() + "\t:\t" + entry.getValue().get());
			}
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			this.collector = arg2;
			this.counterMap = new HashMap<String, AtomicInteger>();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			arg0.declare(new Fields("word", "count"));
		}
	}

	public static void main(String[] args) {
		String zks = "localhost:2181";
		String topic = "my-replicated-topic5";
		String zkRoot = "/storm"; // default zookeeper root configuration for storm
		String id = "word";

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		// spoutConf.forceFromStart = false;
		// spoutConf.f
		spoutConf.zkServers = Arrays.asList(new String[] { "localhost" });
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		builder.setBolt("word-splitter", new KafkaWordSpliter(), 2).shuffleGrouping("kafka-reader");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

		Config conf = new Config();

		String name = KafkaWordCount.class.getSimpleName();
		if (args != null && args.length > 0) {
			// Nimbus host name passed from command line
			conf.put(Config.NIMBUS_HOST, args[0]);
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				logger.error(">>FaceYe Throws Exception:", e);
			} catch (InvalidTopologyException e) {
				logger.error(">>FaceYe Throws Exception:", e);
			} catch (AuthorizationException e) {
				logger.error(">>FaceYe Throws Exception:", e);
			}
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				logger.error(">>FaceYe Throws Exception:", e);
			}
			cluster.shutdown();
		}
	}
}
