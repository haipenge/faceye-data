package com.faceye.component.data.hbase.storage;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.client.HBaseSupport;
import com.faceye.component.data.hbase.wrapper.Page;

public class StockStorage {
	private Logger logger = LoggerFactory.getLogger(StockStorage.class);

	public void save(Example exap){
//		WTable wtable=AnnotationParse.entity2WTable(exap);
		HBaseSupport.getInstance().put(exap);
	}
	
	public void save(List<Example> examples){
		HBaseSupport.getInstance().puts(examples);
	}
	
	public Page<Example> get(Integer start,Integer size){
		List<Example> examples=HBaseSupport.getInstance().getPage(Example.class, null, start, size);
		Page page=new Page<Example>(examples);
		return page;
	}
	
	public Example get(String rowKey){
		Example example=HBaseSupport.getInstance().get(Example.class, rowKey);
		return example;
	}
	
	
	
	public void delete(String rowKey){
		HBaseSupport.getInstance().delete(Example.class,rowKey);
	}

}
