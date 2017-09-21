package com.faceye.component.data.spark.stream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.faceye.component.data.spark.stream.domain.SenderAddress;

public class AddressGenerator {
	List<SenderAddress> addrs = null;

	private static class AddressGeneratorHolder {
		private static final AddressGenerator INSTANCE = new AddressGenerator();
	}

	public static AddressGenerator getInstance() {
		return AddressGeneratorHolder.INSTANCE;
	}
	
	private AddressGenerator(){
		builder();
	}
	
	public SenderAddress getSenderAddress(){
		int index=RandomUtil.get(0, addrs.size());
		return addrs.get(index);
	}

	private void builder() {
		if (addrs == null) {
			addrs = new ArrayList<>();
			String[] lines = read();
			for (String line : lines) {
				String[] splits = line.split("\t");
				SenderAddress addr = new SenderAddress();
				addr.setProvinceCode(splits[0]);
				addr.setCityCode(splits[1]);
				addr.setCountyCode(splits[2]);
				addr.setProvince(splits[3]);
				addr.setCity(splits[4]);
				addr.setCountry(splits[5]);
				addrs.add(addr);
			}
		}
	}

	private String[] read() {
		String text = "";
		StringBuffer buffer = new StringBuffer();
		try {
			BufferedReader in = new BufferedReader(
					new FileReader(AddressGenerator.class.getResource("/city.txt").getPath()));
			String s;
			while ((s = in.readLine()) != null) {
				buffer.append(s);
				buffer.append("\n");
			}
			in.close();
			buffer.append("\n");
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		text = buffer.toString();
		return text.split("\n");
	}
}
