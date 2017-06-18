package com.faceye.component.data.mysql.io;

import java.io.Serializable;

import org.apache.hadoop.mapred.lib.db.DBInputFormat;

/**
 * 数据库导HDFS对像
 * 
 * @author haipenge
 *
 */
public class DBTable implements Serializable {

	/**
	 * @Desc:
	 * @Author:haipenge
	 * @Date:2017年6月18日 下午10:29:15
	 */
	private static final long serialVersionUID = 1L;

	private String table = "";
	private String querySql = "";
	private String countSql = "";
	private Class mapperClass = null;
	private Class reducerClass = null;
	// DBInputFormat.setInput(job, StockRecord.class, TABLE, null, "id", fieldNames);
	private Class dbInputFormatRecordClass = null;// exam:StocmRecord.class
	private String[] fields = null;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getQuerySql() {
		return querySql;
	}

	public void setQuerySql(String querySql) {
		this.querySql = querySql;
	}

	public String getCountSql() {
		return countSql;
	}

	public void setCountSql(String countSql) {
		this.countSql = countSql;
	}

	public Class getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(Class mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(Class reducerClass) {
		this.reducerClass = reducerClass;
	}

	public Class getDbInputFormatRecordClass() {
		return dbInputFormatRecordClass;
	}

	public void setDbInputFormatRecordClass(Class dbInputFormatRecordClass) {
		this.dbInputFormatRecordClass = dbInputFormatRecordClass;
	}

	public String[] getFields() {
		return fields;
	}

	public void setFields(String[] fields) {
		this.fields = fields;
	}

}
