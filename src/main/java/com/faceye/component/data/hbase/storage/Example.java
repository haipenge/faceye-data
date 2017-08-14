package com.faceye.component.data.hbase.storage;

import com.faceye.component.data.hbase.annoation.Column;
import com.faceye.component.data.hbase.annoation.Entity;
import com.faceye.component.data.hbase.annoation.RowKey;

@Entity("example")
public class Example {

	@RowKey
	private String id = "";
	@Column(family = "basic", qualifier = "name")
	private String name = "";
	@Column(family = "basic", qualifier = "sex")
	private String sex = "";
	@Column(family = "other", qualifier = "age")
	private Integer age = 0;
	@Column(family = "other", qualifier = "desc")
	private String desc = "";

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

}
