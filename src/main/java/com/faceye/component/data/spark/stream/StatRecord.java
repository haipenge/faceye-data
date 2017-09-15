package com.faceye.component.data.spark.stream;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

public class StatRecord implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String expOrgCode = "";
	private String province = "";
	private String city = "";
	private String country = "";
	private String checkDate = "";

	public String getExpOrgCode() {
		return expOrgCode;
	}

	public void setExpOrgCode(String expOrgCode) {
		this.expOrgCode = expOrgCode;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCheckDate() {
		return checkDate;
	}

	public void setCheckDate(String checkDate) {
		this.checkDate = checkDate;
	}

	@Override
	public boolean equals(Object obj) {
		boolean res = false;
		if (obj instanceof StatRecord) {
			StatRecord sr = (StatRecord) obj;
			if (StringUtils.equals(sr.getCheckDate(), this.getCheckDate())
					&& StringUtils.equals(sr.getProvince(), this.getProvince())
					&& StringUtils.equals(sr.getCity(), this.getCity())
					&& StringUtils.equals(sr.getCountry(), this.getCountry())
					&& StringUtils.equals(sr.getExpOrgCode(), this.getExpOrgCode())) {
				res = true;
			}
		}

		return res;

	}

}
