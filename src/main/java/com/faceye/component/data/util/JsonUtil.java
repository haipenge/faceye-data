package com.faceye.component.data.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.spark.stream.KafkaStreaming;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtil {
	private static Logger logger = LoggerFactory.getLogger(KafkaStreaming.class);
	private static Gson gson = null;
	static {
		gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
	}

	public static <T> T toObject(String json, Class<T> clazz) {
		T o = null;
		if (StringUtils.isNotEmpty(json)) {
			logger.debug(json);
			if (StringUtils.isNotEmpty(json) && clazz != null) {
				o = gson.fromJson(json, clazz);
			}
		}
		return o;
	}
	public static String toJson(Object obj){
		return gson.toJson(obj);
	}
}
