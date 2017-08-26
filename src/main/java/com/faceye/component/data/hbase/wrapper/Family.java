package com.faceye.component.data.hbase.wrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * 列族
 * @author songhaipeng
 *
 */
public class Family implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String name = "";

	private List<Col> columns = new ArrayList<Col>(0);

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Col> getColumns() {
		return columns;
	}

	public void setColumns(List<Col> columns) {
		this.columns = columns;
	}

	public Col getCol(String key) {
		Col col = null;
		for (Col c : columns) {
			if (StringUtils.equals(c.getKey(), key)) {
				col = c;
				break;
			}
		}
		return col;
	}

}
