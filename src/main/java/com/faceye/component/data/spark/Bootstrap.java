package com.faceye.component.data.spark;

import com.faceye.component.data.spark.stream.KafkaStreaming;

public class Bootstrap {
	public static void main(String[] args) {
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.run();
	}

	public Bootstrap() {
		System.out.println(">>Now,Run bootstrap..");
	}

	public void run() {
//		WordCount wordCount = new WordCount();
//		wordCount.run();
		Run kafka=new KafkaStreaming();
		kafka.run();
	}
}
