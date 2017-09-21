package com.faceye.component.data.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读取配置文件
 * 
 * @author songhaipeng
 *
 */
public class Configuration {
	private static Logger logger = LoggerFactory.getLogger(Configuration.class);
	private static List<String> PROPERTIES_FILES = new ArrayList<String>(0);
	private static Properties props = null;
	static {
		props = new Properties();
		initProperties();
		readProperties();
	}

	/**
	 * 获取配置文件根路径
	 * 
	 * @return
	 */
	public static String getRootDir() {
		URL url = Configuration.class.getClass().getResource("/");
		return url.getPath();
	}

	/**
	 * 根据 Key取得配置
	 * 
	 * @param key
	 * @return
	 */

	public static String get(String key) {
		return props.getProperty(key);
	}

	/**
	 * 根据Key取得配置，若无，返回默认值 defaultValue
	 * 
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static String get(String key, String defaultValue) {
		return props.getProperty(key, defaultValue);
	}

	private static void initProperties() {
		String dir = getRootDir();
		File file = new File(dir);
		File[] files = file.listFiles();
		if (files != null) {
			for (File f : files) {
				if (!f.isDirectory()) {
					String name = f.getName();
					if (StringUtils.endsWithIgnoreCase(name, ".properties")) {
						PROPERTIES_FILES.add(f.getPath());
					}
				}
			}
		}
	}

	private static void readProperties() {
		for (String path : PROPERTIES_FILES) {
			readProperties(path);
		}
	}

	private static void readProperties(String path) {
		try {
			InputStream is = new FileInputStream(path);
			props.load(is);
			is.close();
		} catch (FileNotFoundException e) {
			logger.error(">>Exception:" + e);
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		}
	}

}
