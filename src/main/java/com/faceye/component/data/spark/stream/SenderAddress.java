package com.faceye.component.data.spark.stream;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.lang3.StringUtils;

@XmlAccessorType(XmlAccessType.FIELD)
public class SenderAddress implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@XmlElement(name="ProvinceCode") 
	private String provinceCode = "";
	
	@XmlElement(name="CityCode") 
	private String cityCode = "";
	
	@XmlElement(name="CountyCode") 
	private String countyCode = "";
	
	@XmlElement(name="Street") 
	private String street ;

	public String getProvinceCode() {
		return provinceCode;
	}

	public void setProvinceCode(String provinceCode) {
		this.provinceCode = provinceCode;
	}

	public String getCityCode() {
		return cityCode;
	}

	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}

	public String getCountyCode() {
		return countyCode;
	}

	public void setCountyCode(String countyCode) {
		this.countyCode = countyCode;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public String getStreet() {
		return validLong(street,256);
	}

	public void setStreet(String street) {
		this.street = street;
	}
	
	private String validLong(String arg0,int maxlength){
		if(null != arg0 && arg0.length() > maxlength){
			return StringUtils.substring(arg0, maxlength);
		}
		return arg0;
	}
	
}
