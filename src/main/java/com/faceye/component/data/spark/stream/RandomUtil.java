package com.faceye.component.data.spark.stream;

import java.util.Random;

public class RandomUtil {
	private static Random random = new Random();

	public static int get(int min, int max) {
		int res = min;
		res = min + random.nextInt(max - min);
		return res;
	}
}
