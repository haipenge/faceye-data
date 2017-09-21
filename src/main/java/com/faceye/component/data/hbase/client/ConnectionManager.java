package com.faceye.component.data.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {
	private static Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
	private static Configuration conf = HBaseConfiguration.create();
	private static final String HBASE_ZK_QUORUM = com.faceye.component.data.conf.Configuration.get("hbase.zookeeper.quorum");
	static {
		conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
	}
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>() {
		@Override
		protected Connection initialValue() {
			Connection conn = null;
			try {
				conn = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
			return conn;
		}
	};
	/**
	 * 获取链接
	 * @return
	 */
	public static Connection getConnection() {
		return threadLocal.get();
	}
	/**
	 * 关闭链接
	 */
	public static void close(){
		threadLocal.remove();
	}

}
