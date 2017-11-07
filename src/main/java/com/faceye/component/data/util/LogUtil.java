package com.faceye.component.data.util;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 日志工具类
 * @author songhaipeng
 *
 */
public class LogUtil{
	private static Logger logger=LoggerFactory.getLogger(LogUtil.class);
	public static void start() {
		PropertyConfigurator.configure(LogUtil.class.getClass().getResource("/log4j.properties").getPath());  
	}
	
	public static void main(String[] args) {
		start();
		logger.debug(">>Start logging.");
	}
	
	
}
