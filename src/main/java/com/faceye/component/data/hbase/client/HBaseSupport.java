package com.faceye.component.data.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
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

import com.faceye.component.data.hbase.wrapper.Row;
import com.faceye.component.data.hbase.wrapper.TableReader;
import com.faceye.component.data.hbase.wrapper.WTable;

public class HBaseSupport {
	private Logger logger = LoggerFactory.getLogger(HBaseSupport.class);

	private static class HBaseSupportHolder {
		private static final HBaseSupport INSTANCE = new HBaseSupport();
	}

	public static HBaseSupport getInstance() {
		return HBaseSupportHolder.INSTANCE;
	}

	/**
	 * 保存数据
	 * 
	 * @param wtable
	 */
	public void put(WTable wtable) {
		this.createTableIfTableNotExist(wtable);
		List<Put> puts = TableReader.rows2Puts(wtable.getRows());
		Table table = null;
		try {
			table = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(wtable.getTable()));
			table.put(puts);
		} catch (IOException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(">>FaceYe Throws Exception:", e);
			}
		}
	}

	/**
	 * 获取一行数据, 包含所有列族
	 * 
	 * @param table
	 * @param rowKey
	 * @return
	 * @Desc:
	 * @Author:haipenge
	 * @Date:2017年8月12日 下午4:27:18
	 */
	public Row getRow(String table, String rowKey) {
		Row row = null;
		row = this.getRow(table, rowKey, "");
		return row;
	}

	/**
	 * 取得一行数据，包含指定列族
	 * 
	 * @param table
	 * @param rowKey
	 * @param family
	 * @return
	 * @Desc:
	 * @Author:haipenge
	 * @Date:2017年8月12日 下午4:37:30
	 */
	public Row getRow(String table, String rowKey, String family) {
		Row row = null;
		Table tab = null;
		try {
			if (StringUtils.isEmpty(table)) {
				logger.warn(">>FaceYe error:hbase table name can  not be null when get row.");
			}
			if (StringUtils.isEmpty(rowKey)) {
				logger.warn(">>FaceYe error:hbase rowKey can not be null when get row.");
			}
			tab = HBaseClient.getInstance().getConnection().getTable(TableName.valueOf(table));
			Get get = new Get(Bytes.toBytes(rowKey));
			if (StringUtils.isNotEmpty(family)) {
				get.addFamily(Bytes.toBytes(family));
			}
			Result rs = tab.get(get);
			row = TableReader.result2Row(rs);
		} catch (IOException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		} finally {
			if (tab != null) {
				try {
					tab.close();
				} catch (IOException e) {
					logger.error(">>FaceYe Throws Exception:", e);
				}
			}
		}
		return row;
	}

	/**
	 * 获取分页数据
	 * 
	 * @param table
	 * @param filterList
	 * @param start
	 * @param size
	 * @return
	 * @Desc:
	 * @Author:haipenge
	 * @Date:2017年8月12日 下午4:40:37
	 */
	public List<Row> getRows(String table, FilterList filterList, Integer start, Integer size) {
		List<Row> rows = new ArrayList<Row>(0);
		Table tab = null;
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
			ResultScanner rs;
			rs = tab.getScanner(scan);
			for (Result r : rs) {
				rows.add(TableReader.result2Row(r));
			}
		} catch (IOException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		}
		return rows;
	}

	/**
	 * 如果表不存在，则创建表
	 * 
	 * @param wtable
	 */
	private void createTableIfTableNotExist(WTable wtable) {
		String table = TableReader.getTableName(wtable);
		String[] families = TableReader.getFamilies(wtable);
		HBaseRepository.getInstance().create(table, families);
	}
}
