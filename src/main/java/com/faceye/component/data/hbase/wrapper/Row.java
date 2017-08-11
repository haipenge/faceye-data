package com.faceye.component.data.hbase.wrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Row implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String rowkey="";
	
	private List<Family> families=new ArrayList<Family>(0);

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public List<Family> getFamilies() {
		return families;
	}

	public void setFamilies(List<Family> families) {
		this.families = families;
	}
	
}
