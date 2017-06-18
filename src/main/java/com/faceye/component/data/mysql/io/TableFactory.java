package com.faceye.component.data.mysql.io;

import java.util.ArrayList;
import java.util.List;

public class TableFactory {

	private List<DBTable> tables = null;

	private TableFactory() {
		if (tables == null) {
			tables = new ArrayList<DBTable>(0);
		}
	}

	private static class TableFactoryHolder {
		private final static TableFactory INSTANCE = new TableFactory();
	}

	public synchronized static TableFactory getInstance() {
		return TableFactoryHolder.INSTANCE;
	}

	public List<DBTable> getTables() {
		return tables;
	}

	public void addTable(DBTable dbTable) {
		if (dbTable != null) {
			tables.add(dbTable);
		}
	}
}
