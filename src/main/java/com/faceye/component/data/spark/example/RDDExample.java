package com.faceye.component.data.spark.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark RDD example
 * 
 * @author songhaipeng
 *
 */
public class RDDExample {
	private final static String MASTER = "spark://10.12.12.140:7077";
	private final static String APP_NAME = "rdd-example";

	public static void main(String[] args) {

	}

	public void example() {
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		distData.reduce((a, b) -> a + b);
		// 从物理位置读取
		JavaRDD<String> distFile = sc.textFile("data.txt");
		distFile.map(s -> s.length()).reduce((a, b) -> a + b);
	}
}
