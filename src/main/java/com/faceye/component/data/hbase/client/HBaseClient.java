package com.faceye.component.data.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
	private static final String HBASE_ZK_QUORUM = "localhost";

	private static class HBaseClientHolder {
		private static HBaseClient INSTANCE = new HBaseClient();
	}

	private HBaseClient() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
	}

	public static HBaseClient getInstance() {
		return HBaseClientHolder.INSTANCE;
	}

	public Configuration getConf() {
		return conf;
	}

	public void create(HTableDescriptor htableDesc) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
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
