package com.faceye.test.component.data.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.faceye.component.data.mysql.util.ConnectionManager;

@RunWith(JUnit4.class)
public class DBUtilsTestCase {

	@Test
	public void testGetConnection() throws Exception {
		Connection conn = ConnectionManager.getConnection();
		Assert.assertTrue(conn != null);
	}

	@Test
	public void testBuildConnection() throws Exception {
		Connection connection = null;
		Class.forName("com.mysql.jdbc.Driver");
//		Class.forName("com.mysql.cj.jdbc.Driver");
		connection = DriverManager.getConnection("jdbc:mysql://10.12.12.140:3306/nuc_test?useEncode=true&characterEncoding=utf8", "prnp", "prnp");
		PreparedStatement pstmt=connection.prepareStatement("select * from stream_result");
		Assert.assertTrue(connection!=null);
	}
}
