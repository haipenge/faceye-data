package com.faceye.component.data.hbase.parse;

import java.lang.reflect.Field;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.annoation.Column;
import com.faceye.component.data.hbase.annoation.Entity;
import com.faceye.component.data.hbase.client.RowKeyGenerater;
import com.faceye.component.data.hbase.wrapper.Col;
import com.faceye.component.data.hbase.wrapper.Family;
import com.faceye.component.data.hbase.wrapper.Row;
import com.faceye.component.data.hbase.wrapper.WTable;

public class AnnotationParse {
	private static Logger logger = LoggerFactory.getLogger(AnnotationParse.class);
	
	

	public static WTable entity2WTable(Object entity) {
		WTable wtable = new WTable();
		Row row = new Row();
		row.setRowkey(RowKeyGenerater.getInstance().get());
		if (entity != null) {
			// 获取Entity注解
			Class clazz = entity.getClass();
			Entity entityAnnotation = (Entity) clazz.getAnnotation(Entity.class);
			if (entityAnnotation != null) {
				String table = entityAnnotation.value();
				wtable.setTable(table);
			}
			//获取RowKey注解
//			RowKey rowKeyAnnotaion=(RowKey) field.getAnnotation(RowKey.class);
//			if(rowKeyAnnotation)
			//获取属性上的Column 注解，并组装 为Row对像
			Field[] fields = clazz.getDeclaredFields();
			if (fields != null) {
				for (Field field : fields) {
					Column column = (Column) field.getAnnotation(Column.class);
					if (column != null) {
						String family = column.family();
						Family fam = row.getFamily(family);
						String qualifier = column.qualifier();
						Col col = field2Col(field, entity);
						if (StringUtils.isNotEmpty(qualifier)) {
							col.setKey(qualifier);
						}
						fam.getColumns().add(col);
					}
				}
				wtable.getRows().add(row);
			}
		}
		return wtable;
	}

	private static Col field2Col(Field field, Object entity) {
		Col col = null;
		Class clazz = field.getType();
		String key = "";
		String value = "";
		try {
			if(!field.isAccessible()){
				field.setAccessible(true);
			}
			Object obj = field.get(entity);
			key = field.getName();
			if (obj != null) {
				value = obj.toString();
			}
			col = new Col();
			col.setKey(key);
			col.setValue(value);
		} catch (IllegalArgumentException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		} catch (IllegalAccessException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		}
		return col;
	}

	
	public Object row2Entity(Row row,Class entityClass){
		Object obj=null;
		try {
			obj = entityClass.newInstance();
		} catch (InstantiationException e) {
			logger.error(">>FaceYe Throws Exception:",e);
		} catch (IllegalAccessException e) {
			logger.error(">>FaceYe Throws Exception:",e);
		}
		
		
		return obj;
	}
}
