package com.faceye.component.data.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract public class RunTemplate implements Run{
	protected Logger logger=LoggerFactory.getLogger(getClass());
	
	protected abstract void exec();
	
	@Override
	public void run() {
		logger.debug(">> Start Run.");
		exec();
		logger.debug(">> Finish Run.");
		
	}
}
