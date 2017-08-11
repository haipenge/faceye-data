package com.faceye.component.data.hbase.wrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;

/**
 * 读Table数据
 * @author songhaipeng
 *
 */
public class TableReader {

	public static String getTableName(WTable wtable){
		String tableName=wtable.getTable();
		return tableName;
	}
	public static String [] getFamilies(WTable wtable){
		Set<String> set=new HashSet<String>();
		List<Row> rows=wtable.getRows();
		List<Family> families=new ArrayList<Family>(0);
		for(Row row:rows){
			families.addAll(row.getFamilies());
		}
		for(Family family:families){
			set.add(family.getName());
		}
		return set.toArray(new String[set.size()]);
	}
	
	public Put row2Put(Row row){
		Put put =null;
		String rowKey=row.getRowkey();
		List<Family> families=row.getFamilies();
		for(Family family:families){
			
		}
		return put;
	}
}
