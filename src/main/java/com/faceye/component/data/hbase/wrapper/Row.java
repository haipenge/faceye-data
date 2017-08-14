package com.faceye.component.data.hbase.wrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Row implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String rowkey = "";

	private List<Family> families = new ArrayList<Family>(0);

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

	public Family getFamily(String name) {
		Family res = null;
		for (Family family : families) {
			if (StringUtils.equals(family.getName(), name)) {
				res = family;
				break;
			}
		}
		if(res==null){
			res=new Family();
			res.setName(name);
			families.add(res);
		}
		return res;
	}

	public void addFamily(Family family) {
		if (!isExistFamily(family)) {
			families.add(family);
		}
	}

	private boolean isExistFamily(Family family) {
		boolean res = false;
		for (Family f : families) {
			if (StringUtils.equals(family.getName(), f.getName())) {
				res = true;
				break;
			}
		}
		return res;
	}

}
