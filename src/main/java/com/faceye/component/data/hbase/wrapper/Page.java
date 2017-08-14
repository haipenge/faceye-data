package com.faceye.component.data.hbase.wrapper;

import java.util.List;

public class Page<T> {

	private static final Integer DEFAULT_PAGE_SIZE=50;
	private List<T> items=null;
	private Integer start=0;
	private Integer size=DEFAULT_PAGE_SIZE;
	private Integer count=0;
	public List<T> getItems() {
		return items;
	}
	public void setItems(List<T> items) {
		this.items = items;
	}
	public Integer getStart() {
		return start;
	}
	public void setStart(Integer start) {
		this.start = start;
	}
	public Integer getSize() {
		return size;
	}
	public void setSize(Integer size) {
		this.size = size;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	
	
	
}
