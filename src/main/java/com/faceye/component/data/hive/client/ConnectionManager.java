package com.faceye.component.data.hive.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {
	private static Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
	private static final String URL = "jdbc:hive://10.12.12.140:10000/default";
	private static final String USER = "";
	private static final String PASSWORD = "";
	private static final String DRIVER_CLASS = "org.apache.hadoop.hive.jdbc.HiveDriver";
	PasswdAuthenticationProvider  s=null;
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>() {
		@Override
		protected Connection initialValue() {
			Connection conn = null;
			try {
				conn = DriverManager.getConnection(URL);
			} catch (SQLException e) {
				logger.error(">>Exception:" + e);
			}
			return conn;
		}
	};

	public static Connection getConn() {
		return threadLocal.get();
	}

	public static void close() {
		Connection conn = threadLocal.get();
		try {
			if (conn != null) {
				conn.close();
				threadLocal.remove();
			}
		} catch (SQLException e) {
			logger.error(">>Exception:" + e);
		}

	}
}
