package com.faceye.component.data.spark.stream;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.faceye.component.data.spark.SparkConstants;

import scala.Tuple2;



public class StreamExample {
	public void example(){
		String logFile = "/home/prnp/hadoop/spark-2.2.0-bin-hadoop2.7/README.md"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();
	    long numAs = logData.filter(s -> s.contains("a")).count();
	    long numBs = logData.filter(s -> s.contains("b")).count();
	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    spark.stop();
	}
	
	public void stream() throws InterruptedException{
		SparkConf conf = new SparkConf().setMaster(SparkConstants.MASTER).setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
}
