package com.faceye.component.data.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.access.joran.JoranConfigurator;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.spi.JoranException;

public class Logback {
	private static Logger logger=LoggerFactory.getLogger(Logback.class);
	public static void start() {
		LoggerFactory.getILoggerFactory();
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(lc);
		ch.qos.logback.classic.LoggerContext c=null;
		lc.reset();
		try {
			configurator.doConfigure(Logback.class.getResource("/logback.xml").getPath());
		} catch (JoranException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		start();
		logger.debug(">>Start logging.");
	}
	
	
}
