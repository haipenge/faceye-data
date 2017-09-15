package com.faceye.component.data.mysql.util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtils implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Logger logger = LoggerFactory.getLogger(DBUtils.class);

	private DBUtils(){
		
	}
	private static class DBUtilsHolder{
		private static final DBUtils INSTANCE=new DBUtils();
	}
	public static DBUtils getInstance(){
		return DBUtilsHolder.INSTANCE;
	}
	public void execute(String sql) {
		Connection conn = ConnectionManager.getConnection();
		Statement stat = null;
		logger.debug(">>Sql is:"+sql);
		try {
			stat = conn.createStatement();
			stat.execute(sql);
		} catch (SQLException e) {
			logger.error(">>Exception:" + e);
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					logger.error(">>Exception:" + e);
				}
			}
			if (conn != null) {
				ConnectionManager.close();
			}
		}
	}
	
}
