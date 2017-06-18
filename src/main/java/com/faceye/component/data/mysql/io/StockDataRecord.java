package com.faceye.component.data.mysql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class StockDataRecord implements Writable, DBWritable {

	@Override
	public void readFields(ResultSet arg0) throws SQLException {
	}

	@Override
	public void write(PreparedStatement arg0) throws SQLException {
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	}

}
