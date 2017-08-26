package com.faceye.component.data.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class WordCount {
	public WordCount(){
		System.out.println(">>Run Word count on spark.");
	}
	public void run(){
		String logFile = "/home/prnp/hadoop/spark-2.2.0-bin-hadoop2.7/README.md"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();
	    long numAs = logData.filter(s -> s.contains("a")).count();
	    long numBs = logData.filter(s -> s.contains("b")).count();
	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    spark.stop();
	}
}
