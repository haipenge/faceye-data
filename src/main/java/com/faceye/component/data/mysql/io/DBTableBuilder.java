package com.faceye.component.data.mysql.io;

public class DBTableBuilder {
	
	public static  void builder(){
		
	}
	
	private static DBTable  buildDBTable(String table,String querySql,String countSql,Class mapperClass,Class reducerClass,Class dbInputFormatRecordClass,String [] fields){
		DBTable dbTable =new DBTable();
		dbTable.setTable(table);
		dbTable.setQuerySql(querySql);
		dbTable.setCountSql(countSql);
		dbTable.setMapperClass(mapperClass);
		dbTable.setReducerClass(reducerClass);
		dbTable.setDbInputFormatRecordClass(dbInputFormatRecordClass);
		dbTable.setFields(fields);
		return dbTable;
	}
}
