package com.faceye.component.data.spark.stream;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.kafka.DefaultProducer;
import com.faceye.component.data.kafka.KafkaConstants;
import com.faceye.component.data.spark.stream.domain.RealIDCheckRecord;
import com.faceye.component.data.spark.stream.domain.SenderAddress;
import com.faceye.component.data.util.JsonUtil;
import com.faceye.component.data.util.LogUtil;


/**
 * 构造快件
 * 
 * @author songhaipeng
 *
 */
public class CheckRecoderGenerator {
    private Logger logger=LoggerFactory.getLogger(CheckRecoderGenerator.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	private SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Map EXP_ORG = null;

	/**
	 * 生成快件,每秒1000条
	 */
	public void generator() {
		DefaultProducer producer = new DefaultProducer();
		while (true) {
			List<String> msgs = new ArrayList<>(0);
			for (int i = 0; i < 10; i++) {
				RealIDCheckRecord record = buildRealIDCheckRecord();
				String msg = JsonUtil.toJson(record);
				logger.debug(">>Msg is:"+msg);
				msgs.add(msg);
			}
			producer.send(KafkaConstants.TOPIC_STREAM_EXPRESS_DELIVERY_GENERATE, msgs);
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				logger.error(">>Exception:" + e);
			}
		}

	}
	
	public CheckRecoderGenerator(){
		LogUtil.start();
		buildExpOrg();
	}

	

	private RealIDCheckRecord buildRealIDCheckRecord() {
		Date now = new Date();
		RealIDCheckRecord record = new RealIDCheckRecord();
		record.setCheckDate(sdf.format(now));
		record.setAddTime(sdfDate.format(now));
		record.setCheckMethod(getCheckMethod());
		record.setCreateDate(sdfDate.format(now));
		record.setExpressOrgCode(getExpOrgCode());
		record.setIsPublic(getIsPublic());
		record.setIsReported(getIsReported());
		record.setSenderAddress(buildSenderAddress());
		return record;
	}

	private SenderAddress buildSenderAddress() {
		SenderAddress addr = AddressGenerator.getInstance().getSenderAddress();
		return addr;
	}

	public Map buildExpOrg() {
		if (EXP_ORG == null) {
			EXP_ORG=new HashMap();
			EXP_ORG.put("SF", "顺丰");
			EXP_ORG.put("YTO", "圆通");
			EXP_ORG.put("ZTO", "中通");
			EXP_ORG.put("STO", "申通");
			EXP_ORG.put("YUND", "韵达");
			EXP_ORG.put("BSHT", "百世快递");
			EXP_ORG.put("GTO", "国通");
			EXP_ORG.put("TTKD", "天天快递");
			EXP_ORG.put("EMS", "EMS");
			EXP_ORG.put("UC", "优速");
			EXP_ORG.put("PJ", "品骏");
			EXP_ORG.put("SUR", "速尔");
			EXP_ORG.put("ZGYZ", "邮政集团");
			EXP_ORG.put("JBD", "京东");
			EXP_ORG.put("APE", "全一");
			EXP_ORG.put("rfd", "如风达");
			EXP_ORG.put("KJ", "快捷");
			EXP_ORG.put("GZND", "港中能达");
			EXP_ORG.put("DHL", "DHL");
			EXP_ORG.put("JDKD", "京东快递");
		}
		return EXP_ORG;
	}

	private String getExpOrgCode() {
		String expOrgCode = "";
		int size = EXP_ORG.keySet().size();
		int count = 0;
		int index = RandomUtil.get(0, size);
		Iterator it = EXP_ORG.keySet().iterator();
		while (it.hasNext()) {
			if (count == index) {
				expOrgCode = it.next().toString();
				break;
			}
			count++;
		}
		return expOrgCode;
	}

	private String getCheckMethod() {
		String[] methods = new String[] { "01", "02", "05", "06", "07", "08", "99" };
		int index = RandomUtil.get(0, methods.length);
		return methods[index];
	}

	private String getIsPublic() {
		String[] isPublics = new String[] { "normal", "common" };
		return isPublics[RandomUtil.get(0, 2)];
	}

	private String getIsReported() {
		String[] isReporteds = new String[] { "0", "1" };
		return isReporteds[RandomUtil.get(0, 2)];
	}
	
	public static void main(String[] args) {
		CheckRecoderGenerator g =new CheckRecoderGenerator();
		g.generator();
	}

}
