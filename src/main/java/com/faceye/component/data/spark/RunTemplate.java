package com.faceye.component.data.spark;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract public class RunTemplate implements Run,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected Logger logger=LoggerFactory.getLogger(getClass());
	
	protected abstract void exec();
	
	@Override
	public void run() {
		logger.debug(">> Start Run.");
		exec();
		logger.debug(">> Finish Run.");
		
	}
}
