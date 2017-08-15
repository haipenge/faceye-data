package com.faceye.test.component.data.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.faceye.component.data.hbase.storage.Example;
import com.faceye.component.data.hbase.storage.StockStorage;
import com.faceye.component.data.hbase.wrapper.Page;

@RunWith(JUnit4.class)
public class StockStorageTesetCase {
	StockStorage storage=null;
	@Before
	public void set() throws Exception{
		storage=new StockStorage();
	}
	@After
	public void after() throws Exception{
		storage=null;
	}
	@Test
	public void testCreateTable() throws Exception{
//		StockStorage storage=new StockStorage();
//		storage.create("stock",new String[]{"data"});
//		HBaseAdmin admin=new HBaseAdmin(HBaseClient.getInstance().getConf());
//		TableName tableName = TableName.valueOf("stock");
//		HTableDescriptor htableDesc = new HTableDescriptor(tableName);
//		Assert.assertTrue(admin.tableExists("stock"));
//		admin.close();
	}
	@Test
	public void testPut() throws Exception{
//		String columns[]=new String[]{"code","name"};
//		String values[] =new String[]{"000998","隆平高科"};
//		StockStorage storage=new StockStorage();
//		storage.put("stock", columns, values);
//		Assert.assertTrue(true);
	}
	@Test
	public void testSaveExample() throws Exception{
		
		Example exam=new Example();
		exam.setAge(10);
		exam.setDesc("i am a test");
		exam.setName("test");
		exam.setSex("男");
		storage.save(exam);
		Assert.assertTrue(true);
	}
	@Test
	public void testSaveItems() throws Exception{
		List<Example> exas=new ArrayList<Example>(0);
		for(int i=0;i<100;i++){
			Example exa=new Example();
			exa.setAge(i*10);
			exa.setDesc("desc is:"+i);
			exa.setName("i:"+i);
			exa.setSex(i%2==0?"男":"女");
			exas.add(exa);
		}
		storage.save(exas);
		Page<Example> gets=storage.get(0, 100);
		Assert.assertTrue(gets.getItems().size()==100);
	}
	
	@Test
	public void testGetPage() throws Exception{
		Page<Example> page=storage.get(0, 2);
		Assert.assertTrue(page.getItems().size()==2);
	}
	@Test
	public void testGet() throws Exception{
		Page<Example> page=storage.get(0, 2);
	    Example  example=page.getItems().get(0);
	    String rowKey=example.getId();
	    Example get=storage.get(rowKey);
	    Assert.assertTrue(get!=null &&StringUtils.equals(get.getId(), rowKey)&&get.getSex().equals("男"));
	}
	
	@Test
	public void testDelete() throws Exception{
		Page<Example> page=storage.get(0, 2);
	    Example  example=page.getItems().get(0);
	    String rowKey=example.getId();
	    storage.delete(rowKey);
	    Example get=storage.get(rowKey);
	    Assert.assertTrue(get==null);
	}
	
}
