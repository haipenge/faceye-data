package com.faceye.component.data.hbase.wrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 读Table数据
 * 
 * @author songhaipeng
 *
 */
public class TableReader {

	public static String getTableName(WTable wtable) {
		String tableName = wtable.getTable();
		return tableName;
	}

	public static String[] getFamilies(WTable wtable) {
		Set<String> set = new HashSet<String>();
		List<Row> rows = wtable.getRows();
		List<Family> families = new ArrayList<Family>(0);
		for (Row row : rows) {
			families.addAll(row.getFamilies());
		}
		for (Family family : families) {
			set.add(family.getName());
		}
		return set.toArray(new String[set.size()]);
	}

	/**
	 * 将行结构，转化为将要进行存储的结构 put
	 * @param row
	 * @return
	 */
	public static Put row2Put(Row row) {
		Put put = null;
		put = new Put(Bytes.toBytes(row.getRowkey()));
		List<Family> families = row.getFamilies();
		for (Family family : families) {
			List<Col> columns = family.getColumns();
			for (Col column : columns) {
				String key = column.getKey();
				String value = column.getValue();
				put.addColumn(Bytes.toBytes(family.getName()), Bytes.toBytes(key), Bytes.toBytes(value));
			}
		}
		return put;
	}

	public static List<Put> rows2Puts(List<Row> rows) {
		List<Put> puts = new ArrayList<Put>(0);
		for (Row row : rows) {
			Put put = row2Put(row);
			puts.add(put);
		}
		return puts;
	}

	/**
	 * 将result 结构转化为行结构
	 * @param rs
	 * @return
	 */
	public static Row result2Row(Result rs) {
		Row row = new Row();
		List<Cell> cells = rs.listCells();
		String rowKey=Bytes.toString(rs.getRow());
		row.setRowkey(rowKey);
		if (CollectionUtils.isNotEmpty(cells)) {
			for (Cell cell : cells) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				Family f = row.getFamily(family);
				if (f == null) {
					f = new Family();
					f.setName(family);
					row.addFamily(f);
				}
				String key = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				Col column = new Col();
				column.setKey(key);
				column.setValue(value);
				f.getColumns().add(column);
			}
		}
		return row;
	}
}
