package com.faceye.component.data.hbase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.client.HBaseSupport;

public class StockStorage {
	private Logger logger = LoggerFactory.getLogger(StockStorage.class);

	public void saveExample(Example exap){
//		WTable wtable=AnnotationParse.entity2WTable(exap);
		HBaseSupport.getInstance().put(exap);
	}

}
