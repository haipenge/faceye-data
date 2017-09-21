package com.faceye.component.data.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase客户端操作类
 * 
 * @author songhaipeng
 *
 */
public class HBaseClient {
	private Logger logger = LoggerFactory.getLogger(HBaseClient.class);
	private static Configuration conf = null;
//	private static final String HBASE_ZK_QUORUM = "10.12.12.140";
	private static final String HBASE_ZK_QUORUM = com.faceye.component.data.conf.Configuration.get("hbase.zookeeper.quorum");
//	private static final String HBASE_ZK_QUORUM = "hbase-master";
	private static final String CLIENT_PORT=com.faceye.component.data.conf.Configuration.get("hbase.zookeeper.property.clientPort");

	private static class HBaseClientHolder {
		private static HBaseClient INSTANCE = new HBaseClient();
	}

	private HBaseClient() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
		conf.set("hbase.zookeeper.property.clientPort",CLIENT_PORT); 
	}

	public static HBaseClient getInstance() {
		return HBaseClientHolder.INSTANCE;
	}

	public Configuration getConf() {
		return conf;
	}

	public Connection getConnection() throws IOException {
		Connection conn = null;
		conn = ConnectionFactory.createConnection(conf);
//		Connection conn=ConnectionManager.getConnection();
		return conn;
	}

	public Admin getAdmin() throws IOException {
		Connection conn = getConnection();
		Admin admin = null;
		admin = conn.getAdmin();
		return admin;
	}

	public void create(HTableDescriptor htableDesc) {
		Admin admin = null;
		try {
			admin = getAdmin();
			if (admin.tableExists(htableDesc.getTableName())) {
				logger.debug(">>Table :" + htableDesc.getTableName().getNameAsString() + " is exist now.");
			} else {
				admin.createTable(htableDesc);
			}
		} catch (MasterNotRunningException e) {
			logger.error(">>Exception:" + e);
		} catch (ZooKeeperConnectionException e) {
			logger.error(">>Exception:" + e);
		} catch (IOException e) {
			logger.error(">>Exception:" + e);
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}
}
