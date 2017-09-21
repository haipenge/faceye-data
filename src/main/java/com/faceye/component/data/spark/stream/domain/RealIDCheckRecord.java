package com.faceye.component.data.spark.stream.domain;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.lang3.StringUtils;


@XmlAccessorType(XmlAccessType.FIELD)
public class RealIDCheckRecord  implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * 企业侧 快件状态表主键.
	 */
	private String id;
	
	/**
	 * 上报状态，0未上报，1已上报.
	 */
	private String status;
	
	/**
	 * 用户类型
	 */
	@XmlElement(name="UserType") 
	private String userType = "";
	
	/**
	 * 实名身份伪码
	 */
	@XmlElement(name="IDPseudoCode") 
	private String idPseudoCode = "";
	
	/**
	 * 用户序列号
	 */
	@XmlElement(name="UserId") 
	private String userId = "";
	
	/**
	 * 用户统一标识
	 */
	@XmlElement(name="UserUnifiedId") 
	private String userUnifiedId = "";
	
	
	/**
	 * 组织机构代码
	 */
	@XmlElement(name="OrgCode") 
	private String orgCode;
	
	/**
	 * 统一社会信用代码
	 */
	@XmlElement(name="UnifiedSocialCreditCode") 
	private String unifiedSocialCreditCode;
	
	/**
	 * 税务登记证号
	 */
	@XmlElement(name="TaxRegNo") 
	private String taxRegNo;
	
	/**
	 * 收派员编号,实施用户实名查验动作的收派员编号格式应符合 YZ/T  0143-2015的6.5.26
	 */
	@XmlElement(name="StaffCode") 
	private String staffCode = "";
	
	
	/**
	 * 收派员统一标识
	 */
	@XmlElement(name="StaffUnifiedId") 
	private String staffUnifiedId = "";
	
	
	/**
	 * 查验时间,实施查验的发生时间格式:YYYYMMDDHHmmss
	 */
	@XmlElement(name="CheckDate") 
	private String checkDate = "";
	
	/**
	 * 收派员有效身份证件类型
	 */
	@XmlElement(name="StaffCardType")
	private String StaffCardType;
	/**
	 * 收派员有效身份证件号码
	 */
	@XmlElement(name="StaffCardID")
	private String StaffCardID;
	
	/**
	 * 收派员有效身份证件有效期开始时间
	 */
	@XmlElement(name="StaffCardValidityStartDate")
	private String staffCardValidityStartDate;
	
	/**
	 * 收派员有效身份证件有效期结束时间
	 */
	@XmlElement(name="StaffCardValidityEndDate")
	private String staffCardValidityEndDate;
	
	/**
	 * 查验方式,
	 * 01:首次面验
	 * 02:凭证二维码查验
	 * 03:短信验证码查验
	 * 04:微信验证码查验
	 * 05:有效身份证件查验
	 * 06:身份证识别设备查验
	 * 99:协议客户
	 */
	@XmlElement(name="CheckMethod") 
	private String checkMethod = "";
	
	@XmlElement(name="isPublic") 
	private String isPublic;
	
	@XmlElement(name="SenderAddress") 
	private SenderAddress senderAddress;
	
	/**
	 * 运单号.
	 */
	private String mailNo;
	
	/**
	 * 快递企业组织代码.
	 */
	private String expressOrgCode;
	
	/**
	 * 标记用户是否上报
	 */
	private String isReported;
	
	/**
	 * 用户有效身份证件类型
	 */
	@XmlElement(name="UserCardType")
	private String userIdType = "";
	
	/**
	 * 用户有效身份证件号码
	 */
	@XmlElement(name="UserCardID")
	private String userIdNo = "";
	
	
	/**
	 * 用户有效身份证件有效期开始时间
	 */
	@XmlElement(name="UserCardValidityStartDate")
	private String userCardValidityStartDate;
	
	/**
	 * 用户有效身份证件有效期结束时间
	 */
	@XmlElement(name="UserCardValidityEndDate")
	private String userCardValidityEndDate;
	
	//2017-5-24接口新增字段 fuqiang add
	/**
	 * 发件GPS经纬度
	 */
	@XmlElement(name="SenderLatLng")
	private String senderLatLng = null;
	
	/**
	 * 内件名称
	 */
	@XmlElement(name="InternalsName")
	private String internalsName = null;
	
	/**
	 * 内件类型
	 */
	@XmlElement(name="InternalsType")
	private String internalsType = null;
	
	/**
	 * 内件数量
	 */
	@XmlElement(name="InternalsAmount")
	private int internalsAmount = 0;
	
	/**
	 * 收件人联系电话
	 */
	@XmlElement(name="ReceiverPhone")
	private String receiverPhone = null;
	
	
	private String createDate;
	
	//数据存入kafka的时间
	private String addTime;
	
	private Long checkDateSign;
	
	private String userSign;
	
	
	

	public String getUserSign() {
		return userSign;
	}

	public void setUserSign(String userSign) {
		this.userSign = userSign;
	}

	public String getCreateDate() {
		return filter(createDate);
	}

	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}

	public String getExpressOrgCode() {
		return filter(expressOrgCode);
	}

	public void setExpressOrgCode(String expressOrgCode) {
		this.expressOrgCode = expressOrgCode;
	}

	public String getMailNo() {
		return filter(mailNo);
	}

	public void setMailNo(String mailNo) {
		this.mailNo = mailNo;
	}

	public String getUserId() {
		return filter(userId);
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getStaffCode() {
		return filter(staffCode);
	}

	public void setStaffCode(String staffCode) {
		this.staffCode = staffCode;
	}

	public String getCheckDate() {
		return filter(checkDate);
	}

	public void setCheckDate(String checkDate) {
		this.checkDate = checkDate;
	}

	public String getCheckMethod() {
		return filter(checkMethod);
	}

	public void setCheckMethod(String checkMethod) {
		this.checkMethod = checkMethod;
	}

	public String getUserType() {
		return filter(userType);
	}

	public void setUserType(String userType) {
		this.userType = userType;
	}

	public String getIdPseudoCode() {
		return filter(idPseudoCode);
	}

	public void setIdPseudoCode(String idPseudoCode) {
		this.idPseudoCode = idPseudoCode;
	}

	public SenderAddress getSenderAddress() {
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "YUNDA")){
			senderAddress.setStreet(" ");
		}
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "BSHT")){
			senderAddress.setStreet(" ");
		}
		return senderAddress;
	}

	public void setSenderAddress(SenderAddress senderAddress) {
		this.senderAddress = senderAddress;
	}

	public String getOrgCode() {
		return filter(orgCode);
	}

	public void setOrgCode(String orgCode) {
		this.orgCode = orgCode;
	}

	public String getUnifiedSocialCreditCode() {
		return filter(unifiedSocialCreditCode);
	}

	public void setUnifiedSocialCreditCode(String unifiedSocialCreditCode) {
		this.unifiedSocialCreditCode = unifiedSocialCreditCode;
	}

	public String getStatus() {
		return filter(status);
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getId() {
		return filter(id);
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getIsReported() {
		return filter(isReported);
	}

	public void setIsReported(String isReported) {
		this.isReported = isReported;
	}

	public String getUserIdType() {
		return filter(userIdType);
	}

	public void setUserIdType(String userIdType) {
		this.userIdType = userIdType;
	}

	public String getUserIdNo() {
		return filter(userIdNo);
	}

	public void setUserIdNo(String userIdNo) {
		this.userIdNo = userIdNo;
	}

	public String getStaffCardType() {
		return filter(StaffCardType);
	}

	public void setStaffCardType(String staffCardType) {
		StaffCardType = staffCardType;
	}

	public String getStaffCardID() {
		return filter(StaffCardID);
	}

	public void setStaffCardID(String staffCardID) {
		StaffCardID = staffCardID;
	}

	public String getIsPublic() {
		return filter(isPublic);
	}

	public void setIsPublic(String isPublic) {
		this.isPublic = isPublic;
	}

	public String getUserUnifiedId() {
		return filter(userUnifiedId);
	}

	public void setUserUnifiedId(String userUnifiedId) {
		this.userUnifiedId = userUnifiedId;
	}

	public String getTaxRegNo() {
		return filter(taxRegNo);
	}

	public void setTaxRegNo(String taxRegNo) {
		this.taxRegNo = taxRegNo;
	}

	public String getStaffUnifiedId() {
		return filter(staffUnifiedId);
	}

	public void setStaffUnifiedId(String staffUnifiedId) {
		this.staffUnifiedId = staffUnifiedId;
	}

	public String getStaffCardValidityStartDate() {
		return filter(staffCardValidityStartDate);
	}

	public void setStaffCardValidityStartDate(String staffCardValidityStartDate) {
		this.staffCardValidityStartDate = staffCardValidityStartDate;
	}

	public String getStaffCardValidityEndDate() {
		return filter(staffCardValidityEndDate);
	}

	public void setStaffCardValidityEndDate(String staffCardValidityEndDate) {
		this.staffCardValidityEndDate = staffCardValidityEndDate;
	}

	public String getUserCardValidityStartDate() {
		return filter(userCardValidityStartDate);
	}

	public void setUserCardValidityStartDate(String userCardValidityStartDate) {
		this.userCardValidityStartDate = userCardValidityStartDate;
	}

	public String getUserCardValidityEndDate() {
		return filter(userCardValidityEndDate);
	}

	public void setUserCardValidityEndDate(String userCardValidityEndDate) {
		this.userCardValidityEndDate = userCardValidityEndDate;
	}

	public String getAddTime() {
		return filter(addTime);
	}

	public void setAddTime(String addTime) {
		this.addTime = addTime;
	}

	public String getSenderLatLng() {
		return validLong(filter(senderLatLng),31);
	}

	public void setSenderLatLng(String senderLatLng) {
		this.senderLatLng = senderLatLng;
	}

	public String getInternalsName() {
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "YUNDA")){
			return null;
		}
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "BSHT")){
			return null;
		}
		return validLong(filter(internalsName),32);
	}

	public void setInternalsName(String internalsName) {
		this.internalsName = internalsName;
	}

	public String getInternalsType() {
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "YUNDA")){
			return null;
		}
		return validLong(filter(internalsType),2);
	}

	public void setInternalsType(String internalsType) {
		this.internalsType = internalsType;
	}

	public int getInternalsAmount() {
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "YUNDA")){
			return 1;
		}
		return internalsAmount;
	}

	public void setInternalsAmount(int internalsAmount) {
		this.internalsAmount = internalsAmount;
	}

	public String getReceiverPhone() {
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "YUNDA")){
			return null;
		}
		return validLong(filter(receiverPhone),32);
	}

	public void setReceiverPhone(String receiverPhone) {
		this.receiverPhone = receiverPhone;
	}

	public Long getCheckDateSign() {
		return checkDateSign;
	}

	public void setCheckDateSign(Long checkDateSign) {
		this.checkDateSign = checkDateSign;
	}
	
	private String filter(String arg0){
		return StringUtils.replace(arg0, ",", "");
	}
	
	private String validLong(String arg0,int maxlength){
		if(StringUtils.equalsIgnoreCase(this.getExpressOrgCode(), "YUNDA")){
			return " ";
		}
		if(null != arg0 && arg0.length() > maxlength){
			return StringUtils.substring(arg0, maxlength);
		}
		return arg0;
	}
	
}
