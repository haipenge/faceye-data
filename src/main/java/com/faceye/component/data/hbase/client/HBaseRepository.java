package com.faceye.component.data.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.storage.StockStorage;

/**
 * HBase操作支持 插入 删除 查询
 * 
 * @author songhaipeng
 *
 */
public class HBaseRepository {
	private Logger logger = LoggerFactory.getLogger(HBaseRepository.class);

	private HBaseRepository() {

	}

	private static class RepositorySupportHolder {
		private final static HBaseRepository INSTANCE = new HBaseRepository();
	}

	public static HBaseRepository getInstance() {
		return RepositorySupportHolder.INSTANCE;
	}

	/**
	 * 创建表
	 * 
	 * @param table
	 * @param families
	 */
	public void create(String table, String... families) {
		Admin admin = null;
		try {
			TableName tableName = TableName.valueOf(table);
			HTableDescriptor htableDesc = new HTableDescriptor(tableName);
			for (String family : families) {
				htableDesc.addFamily(new HColumnDescriptor(family));
			}
			admin = HBaseClient.getInstance().getAdmin();
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

	/**
	 * 在指定列族上写数据
	 * 
	 * @param tableName
	 * @param family
	 * @param columns
	 * @param values
	 */
	public void put(String tableName, String family, String columns[], String values[]) {
		Table table = null;
		try {
			table = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(tableName));
			Put put = this.buildPut(table, family, RowKeyGenerater.getInstance().get(), columns, values);
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

	/**
	 * 在多个列族上同时写数据
	 * 
	 * @param table
	 * @param families
	 * @param columns
	 * @param values
	 */
	public void put(String table, String[] families, List<String[]> columns, List<String[]> values) {
		if (families != null && columns != null && values != null && families.length == columns.size()
				&& columns.size() == values.size()) {
			Table tab = null;
			List<Put> puts = new ArrayList<Put>(0);
			try {
				tab = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(table));
				String rowKey = RowKeyGenerater.getInstance().get();
				for (int i = 0; i < families.length; i++) {
					String family = families[i];
					String[] column = columns.get(i);
					String[] value = values.get(i);
					Put put = this.buildPut(tab, rowKey, family, column, value);
					if (put != null) {
						puts.add(put);
					}
				}
				if (CollectionUtils.isNotEmpty(puts)) {
					tab.put(puts);
				}
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			} finally {
				try {
					tab.close();
				} catch (IOException e) {
					logger.error(">>Exception:" + e);
				}
			}
		} else {
			logger.error(">>Family size not equal with columns size or values size.");
		}
	}

	/**
	 * 在指定列族上批量写数据
	 * 
	 * @param tableName
	 * @param family
	 * @param columnItems
	 * @param valueItems
	 */
	public void batchPut(String tableName, String family, List<String[]> columnItems, List<String[]> valueItems) {
		Table table = null;
		List<Put> puts = new ArrayList<Put>(0);
		try {
			table = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(tableName));
			if (columnItems != null && valueItems != null && columnItems.size() == valueItems.size()) {
				int length = columnItems.size();
				for (int i = 0; i < length; i++) {
					String[] columns = columnItems.get(i);
					String[] values = valueItems.get(i);
					String rowKey = RowKeyGenerater.getInstance().get();
					Put put = this.buildPut(table, rowKey, family, columns, values);
					if (put != null) {
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

	private Put buildPut(Table table, String rowKey, String family, String columns[], String values[]) {
		HColumnDescriptor[] columnFamilies;
		Put put = null;
		try {
			if (columns != null && values != null && columns.length == values.length) {
				columnFamilies = table.getTableDescriptor().getColumnFamilies();
				int columnLength = columns.length;
				// RowKey
				put = new Put(Bytes.toBytes(rowKey));
				for (HColumnDescriptor columnDescriptor : columnFamilies) {
					String columnFamily = columnDescriptor.getNameAsString();
					if (StringUtils.equals(columnFamily, family)) {
						for (int i = 0; i < columnLength; i++) {
							String column = columns[i];
							String value = values[i];
							// Cell cell = new
							// KeyValue(Bytes.toBytes(columnFamily),
							// Bytes.toBytes(column), Bytes.toBytes(value));
							// put.add(cell);
							put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
						}
					}
				}
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		}
		return put;
	}

	/**
	 * 删除 表
	 * 
	 * @param table
	 */
	public void drop(String table) {
		TableName tableName = TableName.valueOf(table);
		Admin admin = null;
		try {
			admin = HBaseClient.getInstance().getAdmin();
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			logger.error(">>Exception:" + e);
		} catch (ZooKeeperConnectionException e) {
			logger.error(">>Exception:" + e);
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.error(">>Exception:" + e);
				}
			}
		}
	}

	/**
	 * 删除一行、或一行的某个列族，或是一个列族的某一列值
	 * 
	 * @param table
	 * @param rowKey
	 * @param familyColumn
	 * @param qualifier
	 */
	public void delete(String table, String rowKey, String family, String qualifier) {
		TableName tableName = TableName.valueOf(table);
		Table tab = null;
		try {
			if (StringUtils.isNotEmpty(rowKey)) {
				tab = HBaseClient.getInstance().getConnection().getTable(tableName);
				Delete delete = new Delete(Bytes.toBytes(rowKey));
				if (StringUtils.isNotEmpty(family)) {
					delete.addFamily(Bytes.toBytes(family));
				}
				if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(qualifier)) {
					delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				}
				tab.delete(delete);
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				tab.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}

	/**
	 * 删除多行数据
	 * 
	 * @param table
	 * @param rowKeys
	 */
	public void remove(String table, List<String> rowKeys) {
		TableName tableName = TableName.valueOf(table);
		Table tab = null;
		try {
			List<Delete> deletes = new ArrayList<Delete>(0);
			tab = HBaseClient.getInstance().getConnection().getTable(tableName);
			if (CollectionUtils.isNotEmpty(rowKeys)) {
				for (String rowKey : rowKeys) {
					if (StringUtils.isNotEmpty(rowKey)) {
						Delete delete = new Delete(Bytes.toBytes(rowKey));
						deletes.add(delete);
					}
				}
				tab.delete(deletes);
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				tab.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}

	/**
	 * 取得指定行一列的值
	 * 
	 * @param table
	 * @param rowKey
	 * @param familyColumn
	 * @param qualifier
	 * @return
	 */
	public String get(String table, String rowKey, String family, String qualifier) {
		String res = "";
		Table tab = null;
		try {
			tab = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(table));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			Result rs = tab.get(get);
			if (!rs.isEmpty()) {
				res = Bytes.toString(CellUtil.cloneValue(rs.listCells().get(0)));
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				tab.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
		return res;
	}

	/**
	 * 取得一行-》列族的值
	 * 
	 * @param table
	 * @param rowKey
	 * @param familyColumn
	 * @return
	 */
	public Map<String, String> get(String table, String rowKey, String family) {
		Table tab = null;
		Map<String, String> map = null;
		try {
			tab = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(table));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Bytes.toBytes(family));
			Result rs = tab.get(get);
			if (!rs.isEmpty()) {
				List<Cell> cells = rs.listCells();
				if (CollectionUtils.isNotEmpty(cells)) {
					map = new HashMap<String, String>();
					for (Cell cell : cells) {
						String key = Bytes.toString(CellUtil.cloneQualifier(cell));
						String value = Bytes.toString(CellUtil.cloneValue(cell));
						map.put(key, value);
					}
				}
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				tab.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
		return map;
	}

	/**
	 * 取得一行的值，含多个列族
	 * 
	 * @param table
	 * @param rowKey
	 */
	public Map<String, Map<String, String>> get(String table, String rowKey) {
		Map<String, Map<String, String>> map = null;
		Table tab = null;
		try {
			tab = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(table));
			Get get = new Get(Bytes.toBytes(rowKey));
			Result rs = tab.get(get);
			if (!rs.isEmpty()) {
				map = wrapRow(rs);
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				tab.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
		return map;
	}

	/**
	 * 包装某一RowKey数据
	 * 
	 * @param rs
	 * @return
	 */
	private Map<String, Map<String, String>> wrapRow(Result rs) {
		Map<String, Map<String, String>> map = null;
		List<Cell> cells = rs.listCells();
		if (CollectionUtils.isNotEmpty(cells)) {
			map = new HashMap<String, Map<String, String>>();
			for (Cell cell : cells) {
				String familyColumn = Bytes.toString(CellUtil.cloneFamily(cell));
				if (!map.containsKey(familyColumn)) {
					map.put(familyColumn, new HashMap<String, String>());
				}
				String key = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				map.get(familyColumn).put(key, value);
			}
		}
		return map;
	}

	/**
	 * 通用查询
	 * 
	 * @param table
	 * @param filterList
	 */
	public List<Map<String, Map<String, String>>> get(String table, FilterList filterList) {
		List<Map<String, Map<String, String>>> result = get(table, filterList, null, null);
		return result;
	}

	/**
	 * 分页查询
	 * 
	 * @param table
	 * @param filterList
	 * @param start
	 * @param size
	 * @return
	 */
	public List<Map<String, Map<String, String>>> get(String table, FilterList filterList, Integer start,
			Integer size) {
		Table tab = null;
		List<Map<String, Map<String, String>>> result = new ArrayList<Map<String, Map<String, String>>>(0);
		try {
			tab = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(table));
			Scan scan = new Scan();
			if (start != null && start >= 0) {
				scan.setStartRow(Bytes.toBytes(start));
			}
			if (size != null && size > 0) {
				Filter pageFilter = new PageFilter(size);
				if (filterList != null) {
					filterList.addFilter(pageFilter);
				} else {
					scan.setFilter(pageFilter);
				}
			}
			scan.setFilter(filterList);
			ResultScanner rs = tab.getScanner(scan);
			for (Result r : rs) {
				result.add(wrapRow(r));
			}
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				tab.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
		return result;
	}
}
