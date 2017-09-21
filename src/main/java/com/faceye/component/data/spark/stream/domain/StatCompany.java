package com.faceye.component.data.spark.stream.domain;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

/**
 * 根据企业维度进行统计
 * 
 * @author songhaipeng
 *
 */
public class StatCompany implements Serializable {

	/**
	 * 企业代码
	 */
	private String expOrgCode = "";
	/**
	 * 查验时间
	 */
	private String checkDate = "";
	/**
	 * 是否已关联
	 */
	private String isReported = "";
	/**
	 * 总数
	 */
	private Integer total = 0;

	public String getExpOrgCode() {
		return expOrgCode;
	}

	public void setExpOrgCode(String expOrgCode) {
		this.expOrgCode = expOrgCode;
	}

	public String getCheckDate() {
		return checkDate;
	}

	public void setCheckDate(String checkDate) {
		this.checkDate = checkDate;
	}

	public String getIsReported() {
		return isReported;
	}

	public void setIsReported(String isReported) {
		this.isReported = isReported;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	@Override
	public boolean equals(Object obj) {
		boolean res = false;
		if (obj instanceof StatCompany) {
			StatCompany sc = (StatCompany) obj;
			res = StringUtils.equals(this.getExpOrgCode(), sc.getExpOrgCode())
					&& StringUtils.equals(this.checkDate, sc.getCheckDate())
					&& StringUtils.equals(this.getIsReported(), sc.getIsReported());
		}
		return res;
	}

}
