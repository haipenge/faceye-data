package com.faceye.test.component.data.hbase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.faceye.component.data.hbase.storage.Example;
import com.faceye.component.data.hbase.storage.StockStorage;

@RunWith(JUnit4.class)
public class StockStorageTesetCase {

	@Before
	public void set() throws Exception{
		
	}
	@After
	public void after() throws Exception{
		
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
		StockStorage storage=new StockStorage();
		Example exam=new Example();
		exam.setAge(10);
		exam.setDesc("i am a test");
		exam.setName("test");
		exam.setSex("男");
		storage.saveExample(exam);
		Assert.assertTrue(true);
	}
}
