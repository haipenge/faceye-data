package com.faceye.component.data.spark.stream.output;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.mysql.util.ConnectionManager;
import com.faceye.component.data.spark.stream.domain.StatCompany;
import com.faceye.component.data.spark.stream.domain.StatRecord;

/**
 * 输出Spark streaming计算结果
 * 
 * @author songhaipeng
 *
 */
public class StreamingOutput {
	private static Logger logger = LoggerFactory.getLogger(StreamingOutput.class);

	/**
	 * 输出快件统计结果 全量统计条件
	 * 
	 * @param statResults
	 */
	public static void outputExpressDeliveryStatusStreamingResult(List<StatRecord> statRecords) {
		List<StatRecord> insertStatRecords = new ArrayList<>(0);
		List<StatRecord> updateStatRecords = new ArrayList<>(0);
		for (StatRecord sr : statRecords) {
			boolean isExist = isStatRecordExist(sr);
			if (isExist) {
				updateStatRecords.add(sr);
			} else {
				insertStatRecords.add(sr);
			}
		}
		if (CollectionUtils.isNotEmpty(insertStatRecords)) {
			insertStatRecords(insertStatRecords);
		}
		if (CollectionUtils.isNotEmpty(updateStatRecords)) {
			updateStatRecords(updateStatRecords);
		}
	}

	/**
	 * 插入操作
	 * 
	 * @param statRecords
	 */
	private static void insertStatRecords(List<StatRecord> statRecords) {
		String sql = "insert into stream_result (exp_org_code,province,city,country,check_date,total,check_method,is_reported) values(?,?,?,?,?,?,?,?);";
		PreparedStatement pstmt = null;
		Connection conn = ConnectionManager.getConnection();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			for (StatRecord sr : statRecords) {
				pstmt.setString(1, sr.getExpOrgCode());
				pstmt.setInt(2, NumberUtils.toInt(sr.getProvince()));
				pstmt.setInt(3, NumberUtils.toInt(sr.getCity()));
				pstmt.setInt(4, NumberUtils.toInt(sr.getCountry()));
				pstmt.setInt(5, NumberUtils.toInt(sr.getCheckDate()));
				pstmt.setInt(6, sr.getTotal());
				pstmt.setString(7, sr.getCheckMethod());
				pstmt.setInt(8, NumberUtils.toInt(sr.getIsReported()));
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			logger.error(">>Exception:" + e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					logger.error(">>Exception:" + e);
				}
			}
			if (conn != null) {
				ConnectionManager.close();
			}
		}
	}

	private static void updateStatRecords(List<StatRecord> statRecords) {
		String sql = "update stream_result set total = total + ? where exp_org_code=? and province=? and city=? and country=?  and check_date =? and check_method=? and is_reported=?";
		PreparedStatement pstmt = null;
		Connection conn = ConnectionManager.getConnection();
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			for (StatRecord sr : statRecords) {
				pstmt.setInt(1, sr.getTotal());
				pstmt.setString(2, sr.getExpOrgCode());
				pstmt.setInt(3, NumberUtils.toInt(sr.getProvince()));
				pstmt.setInt(4, NumberUtils.toInt(sr.getCity()));
				pstmt.setInt(5, NumberUtils.toInt(sr.getCountry()));
				pstmt.setInt(6, NumberUtils.toInt(sr.getCheckDate()));
				pstmt.setString(7, sr.getCheckMethod());
				pstmt.setInt(8, NumberUtils.toInt(sr.getIsReported()));
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			logger.error(">>Exception:" + e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					logger.error(">>Exception:" + e);
				}
			}
			if (conn != null) {
				ConnectionManager.close();
			}
		}
	}

	private static boolean isStatRecordExist(StatRecord sr) {
		boolean res = false;
		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		String sql = "select total from stream_result where exp_org_code=? and province=? and city=? and country=? and check_date =?";
		// and city=? and country=?
		try {
			conn = ConnectionManager.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, NumberUtils.toInt(sr.getExpOrgCode()));
			pstmt.setInt(2, NumberUtils.toInt(sr.getProvince()));
			pstmt.setInt(3, NumberUtils.toInt(sr.getCity()));
			pstmt.setInt(4, NumberUtils.toInt(sr.getCountry()));
			pstmt.setInt(5, NumberUtils.toInt(sr.getCheckDate()));
			// stat = conn.createStatement();
			// rs = stat.executeQuery(sql);
			rs = pstmt.executeQuery();
			res = rs.next();
		} catch (SQLException e) {
			logger.error(">>Exception:" + e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(">>Exception:" + e);
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					logger.error(">>Exception:" + e);
				}
			}
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

		return res;
	}

	/**
	 * 按企业维度进行统计
	 * 
	 * @param results
	 */
	public static void outputStreamResultOfCompany(List<StatCompany> results) {
		if (CollectionUtils.isNotEmpty(results)) {
			List<StatCompany> inserts=new ArrayList<StatCompany>();
			List<StatCompany> updates=new ArrayList<>();
			for (StatCompany sc : results) {
              boolean isExist=isStatCompnayExist(sc);
              if(isExist){
            	  updates.add(sc);
              }else{
            	  inserts.add(sc);
              }
			}
			insertStatCompany(inserts);
			updateStatCompany(updates);
		}
	}

	private static boolean isStatCompnayExist(StatCompany sc) {
		boolean res = false;
		if (sc != null) {
			String sql = "select * from stat_stream_res_compay sc where sc.exp_org_code=? and sc.check_date=? and sc.is_reported=?";
			Connection conn = null;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try {
				conn = ConnectionManager.getConnection();
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, sc.getExpOrgCode());
				pstmt.setInt(2, NumberUtils.toInt(sc.getCheckDate()));
				pstmt.setInt(3, NumberUtils.toInt(sc.getIsReported()));
				rs = pstmt.executeQuery();
				res = rs.next();
			} catch (SQLException e) {
				logger.error(">>Exception:" + e);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						logger.error(">>Exception:" + e);
					}
				}
				if (pstmt != null) {
					try {
						pstmt.close();
					} catch (SQLException e) {
						logger.error(">>Exception:" + e);
					}
				}
				if (conn != null) {
					ConnectionManager.close();
				}
			}
		}
		return res;
	}

	private static void insertStatCompany(List<StatCompany> scs) {
		String sql = "insert into stat_stream_res_compay(exp_org_code,check_date,is_reported,total) values(?,?,?,?)";
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = ConnectionManager.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			for (StatCompany sc : scs) {
				pstmt.setString(1, sc.getExpOrgCode());
				pstmt.setInt(2, NumberUtils.toInt(sc.getCheckDate()));
				pstmt.setInt(3, NumberUtils.toInt(sc.getIsReported()));
				pstmt.setInt(4, sc.getTotal());
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			logger.error(">>>E:" + e);
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					logger.error(">>Exception:" + e);
				}
			}
			if (conn != null) {
				ConnectionManager.close();
			}
		}
	}

	private static void updateStatCompany(List<StatCompany> scs) {
		if (CollectionUtils.isNotEmpty(scs)) {
			Connection conn = null;
			PreparedStatement pstmt = null;
			String sql = "update stat_stream_res_compay set total =total + ? where exp_org_code =? and check_date =?  and is_reported=?";
			try {
				conn = ConnectionManager.getConnection();
				conn.setAutoCommit(false);
				pstmt = conn.prepareStatement(sql);
				for (StatCompany sc : scs) {
					pstmt.setInt(1, sc.getTotal());
					pstmt.setString(2, sc.getExpOrgCode());
					pstmt.setInt(3, NumberUtils.toInt(sc.getCheckDate()));
					pstmt.setInt(4, NumberUtils.toInt(sc.getIsReported()));
					pstmt.addBatch();
				}
				pstmt.executeBatch();
				conn.commit();
			} catch (Exception e) {
				logger.error(">>>" + e);
			} finally {
				if (pstmt != null) {
					try {
						pstmt.close();
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
}
