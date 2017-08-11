package com.faceye.component.data.hbase.client;

import java.util.List;

import com.faceye.component.data.hbase.wrapper.Row;
import com.faceye.component.data.hbase.wrapper.TableReader;
import com.faceye.component.data.hbase.wrapper.WTable;

public class HBaseSupport {
	
	
	/**
	 * 保存数据
	 * @param wtable
	 */
	public void put(WTable wtable){
		this.createTableIfTableNotExist(wtable);
		List<Row> rows =wtable.getRows();
		for(Row row:rows){
			
		}
	}
	
	
	
	/**
	 * 如果表不存在，则创建表
	 * @param wtable
	 */
	private void createTableIfTableNotExist(WTable wtable){
		String table =TableReader.getTableName(wtable);
		String [] families=TableReader.getFamilies(wtable);
		HBaseRepository.getInstance().create(table, families);
	}
}
