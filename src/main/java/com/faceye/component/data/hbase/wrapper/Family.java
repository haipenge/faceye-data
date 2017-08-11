package com.faceye.component.data.hbase.wrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Family implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String name = "";

	private List<Column> columns = new ArrayList<Column>(0);

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
