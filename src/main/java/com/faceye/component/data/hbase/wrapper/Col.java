package com.faceye.component.data.hbase.wrapper;

import java.io.Serializable;
/**
 * 列对像
 * @author songhaipeng
 *
 */
public class Col implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 列修饰符
	 */
	private String key="";
	/**
	 * 列值,key,value共同组成hbase->row->family->cell
	 */
	private String value="";
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
}
