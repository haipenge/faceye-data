package com.faceye.component.data.mysql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;


public class StockRecord implements Writable, DBWritable {

	private String id;
	private String name = "";
	private String code = "";
	private String stock_trade_id="";

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}
	

	public String getStock_trade_id() {
		return stock_trade_id;
	}

	public void setStock_trade_id(String stock_trade_id) {
		this.stock_trade_id = stock_trade_id;
	}

	@Override
	public void readFields(ResultSet arg0) throws SQLException {
		this.setName(arg0.getString("name"));
		this.setId(arg0.getString("id"));
		this.setCode(arg0.getString("code"));
		this.setStock_trade_id(arg0.getString("stock_trade_id"));
	}

	@Override
	public void write(PreparedStatement arg0) throws SQLException {
		arg0.setString(1, id);
		arg0.setString(2, name);
		arg0.setString(3, code);
		arg0.setString(4, this.stock_trade_id);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.id = Text.readString(arg0);
		this.name = Text.readString(arg0);
		this.code = Text.readString(arg0);
		this.stock_trade_id=Text.readString(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		Text.writeString(arg0, this.getId());
		Text.writeString(arg0, this.getName());
		Text.writeString(arg0, this.getCode());
		Text.writeString(arg0, this.stock_trade_id);
	}
	
	@Override  
    public String toString() {  
         return this.id + " " + this.name + " " + this.code+" "+this.stock_trade_id;    
    }  

}
