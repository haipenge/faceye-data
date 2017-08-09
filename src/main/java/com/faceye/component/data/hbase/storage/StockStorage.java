package com.faceye.component.data.hbase.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.client.HBaseClient;
import com.faceye.component.data.hbase.client.RowKeyGenerater;

public class StockStorage {
	private Logger logger = LoggerFactory.getLogger(StockStorage.class);
	private static String TABLE_NAME = "stock";

	public void createStockTable() {
		HBaseAdmin admin = null;
		try {
			TableName tableName = TableName.valueOf(TABLE_NAME);
			HTableDescriptor htableDesc = new HTableDescriptor(tableName);
			String[] families = new String[] { "data" };
			for (String family : families) {
				htableDesc.addFamily(new HColumnDescriptor(family));
			}
			admin = new HBaseAdmin(HBaseClient.getInstance().getConf());
			if (admin.tableExists(htableDesc.getTableName())) {
				logger.debug(">>Table :" + htableDesc.getTableName().getNameAsString() + " is exist now.");
			} else {
				admin.createTable(htableDesc);
			}
		} catch (MasterNotRunningException e) {
			logger.error(">>Exception:" + e);
		} catch (ZooKeeperConnectionException e) {
			logger.error(">>Exception:" + e);
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}

	public void put(String columns[], String values[]) {
		HTable table = null;
		try {
			table = new HTable(HBaseClient.getInstance().getConf(), Bytes.toBytes(TABLE_NAME));
			Put put = this.buildPut(table, columns, values);
			if (put != null) {
				table.put(put);
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}

	public void batchPut(List<String[]> columnItems, List<String[]> valueItems) {
		HTable table = null;
		List<Put> puts = new ArrayList<Put>(0);
		try {
			table = new HTable(HBaseClient.getInstance().getConf(), Bytes.toBytes(TABLE_NAME));
			if (columnItems != null && valueItems != null && columnItems.size() == valueItems.size()) {
				int length = columnItems.size();
				for (int i = 0; i < length; i++) {
					String[] columns = columnItems.get(i);
					String[] values = valueItems.get(i);
					Put put=this.buildPut(table, columns, values);
					if(put!=null){
						puts.add(put);
					}
				}
				table.put(puts);
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}

	private Put buildPut(HTable table, String columns[], String values[]) {
		HColumnDescriptor[] columnFamilies;
		Put put = null;
		try {
			if (columns != null && values != null && columns.length == values.length) {
				columnFamilies = table.getTableDescriptor().getColumnFamilies();
				int columnLength = columns.length;
				// 设置RowKey
				put = new Put(Bytes.toBytes(RowKeyGenerater.getInstance().get().toString()));
				for (HColumnDescriptor columnDescriptor : columnFamilies) {
					String columnFamily = columnDescriptor.getNameAsString();
					if (StringUtils.equals(columnFamily, "data")) {
						for (int i = 0; i < columnLength; i++) {
							String column = columns[i];
							String value = values[i];
							Cell cell = new KeyValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
									Bytes.toBytes(value));
							put.add(cell);
						}
					}
				}
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} // 获取所有的列族

		return put;
	}

}
