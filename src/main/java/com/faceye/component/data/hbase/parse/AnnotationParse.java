package com.faceye.component.data.hbase.parse;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.annoation.Column;
import com.faceye.component.data.hbase.annoation.Entity;
import com.faceye.component.data.hbase.annoation.RowKey;
import com.faceye.component.data.hbase.client.RowKeyGenerater;
import com.faceye.component.data.hbase.wrapper.Col;
import com.faceye.component.data.hbase.wrapper.Family;
import com.faceye.component.data.hbase.wrapper.Row;
import com.faceye.component.data.hbase.wrapper.WTable;

/**
 * HBase实体注解解释器
 * 
 * @author songhaipeng
 *
 */
public class AnnotationParse {
	private static Logger logger = LoggerFactory.getLogger(AnnotationParse.class);

	public static <T> WTable entity2WTable(T entity) {
		WTable wtable = null;
		if (entity != null) {
			wtable = new WTable();
			Class clazz = entity.getClass();
			String table = getTable(clazz);
			wtable.setTable(table);
			Row row = entity2Row(entity);
			row.setRowkey(RowKeyGenerater.getInstance().get());
			wtable.getRows().add(row);
		}
		return wtable;
	}

	/**
	 * 将一组对像转化为WTable,以支持批量保存
	 * 
	 * @param entities
	 * @return
	 */
	public static <T> WTable entities2WTable(Iterable<T> entities) {
		WTable wtable = null;
		if (entities != null) {
			for (T entity : entities) {
				if (wtable == null) {
					wtable = entity2WTable(entity);
				} else {
					Row row = entity2Row(entity);
					wtable.getRows().add(row);
				}
			}
		}
		return wtable;
	}

	/**
	 * 取得对像对应的table
	 * 
	 * @param clazz
	 * @return
	 */
	public static String getTable(Class clazz) {
		String table = "";
		Entity entityAnnotation = (Entity) clazz.getAnnotation(Entity.class);
		if (entityAnnotation != null) {
			table = entityAnnotation.value();
		}
		return table;
	}

	/**
	 * 获取Rowkey
	 * 
	 * @param entity
	 * @return
	 */
	public static <T> String getRowKey(T entity) {
		String rowKey = "";
		Field[] fields = getFields(entity.getClass());
		for (Field field : fields) {
			Annotation annotation = getAnnotationOfField(field, RowKey.class);
			if (annotation != null) {
				if (!field.isAccessible()) {
					field.setAccessible(true);
				}
				Object obj;
				try {
					obj = field.get(entity);
					rowKey = obj == null ? "" : obj.toString();
				} catch (IllegalArgumentException e) {
					logger.error(">>Exception:" + e);
				} catch (IllegalAccessException e) {
					logger.error(">>Exception:" + e);
				}

			}
		}
		return rowKey;
	}

	/**
	 * 将一个对像转化为一条记录
	 * 
	 * @param entity
	 * @return
	 */
	private static <T> Row entity2Row(T entity) {
		Row row = null;
		if (entity != null) {
			Class clazz = entity.getClass();
			row = new Row();
			String rowKey = getRowKey(entity);
			if (StringUtils.isEmpty(rowKey)) {
				row.setRowkey(RowKeyGenerater.getInstance().get());
			}
			Field[] fields = clazz.getDeclaredFields();
			if (fields != null) {
				for (Field field : fields) {
					Column column = (Column) field.getAnnotation(Column.class);
					if (column != null) {
						String family = column.family();
						Family fam = row.getFamily(family);
						String qualifier = column.qualifier();
						Col col = field2Col(field, entity);
						if (col != null && StringUtils.isNotEmpty(qualifier)) {
							col.setKey(qualifier);
							fam.getColumns().add(col);
						}

					}
				}
			}
		}
		return row;
	}

	private static Col field2Col(Field field, Object entity) {
		Col col = null;
		Class clazz = field.getType();
		String key = "";
		String value = "";
		try {
			if (!field.isAccessible()) {
				field.setAccessible(true);
			}
			Object obj = field.get(entity);
			key = field.getName();
			if (obj != null) {
				value = obj.toString();
				col = new Col();
				col.setKey(key);
				col.setValue(value);
			}

		} catch (IllegalArgumentException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		} catch (IllegalAccessException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		}
		return col;
	}

	public static <T> List<T> rows2Entities(List<Row> rows, Class entityClass) {
		List<T> entities = new ArrayList<T>(0);
		for (Row row : rows) {
			T entity = row2Entity(row, entityClass);
			entities.add(entity);
		}
		return entities;
	}

	/**
	 * 将HBase一行记录，转化为Entity对像
	 * 
	 * @param row
	 * @param entityClass
	 * @return
	 */
	public static <T> T row2Entity(Row row, Class entityClass) {
		T obj = null;
		try {
			if (row != null) {
				obj = (T) entityClass.newInstance();
				String rowKey = row.getRowkey();
				Field[] fields = getFields(entityClass);
				for (Field field : fields) {
					Annotation annotation = getAnnotationOfField(field, RowKey.class);
					if (annotation != null) {
						if (!field.isAccessible()) {
							field.setAccessible(true);
						}
						field.set(obj, rowKey);
					}
					Annotation columnAnnotation = getAnnotationOfField(field, Column.class);
					if (columnAnnotation != null) {
						Column column = (Column) columnAnnotation;
						String family = column.family();
						String qualifier = column.qualifier();
						Family f = row.getFamily(family);
						Col col = f.getCol(qualifier);
						if (!field.isAccessible() && col != null) {
							field.setAccessible(true);
							setObjectField(field, obj, col.getValue());
						}

					}
				}
			}
		} catch (InstantiationException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		} catch (IllegalAccessException e) {
			logger.error(">>FaceYe Throws Exception:", e);
		}
		return obj;
	}

	/**
	 * 为对像设置 值
	 * 
	 * @param field
	 * @param obj
	 * @param value
	 */
	private static void setObjectField(Field field, Object obj, Object value) {
		if (field != null && obj != null && value != null) {
			Class clazz = field.getType();
			try {
				if (StringUtils.equals(clazz.getName(), String.class.getName())) {
					field.set(obj, value.toString());
				} else if (StringUtils.equals(clazz.getName(), Integer.class.getName())) {
					field.setInt(obj, NumberUtils.toInt(value.toString()));
				} else if (StringUtils.equals(clazz.getName(), Double.class.getName())
						|| StringUtils.equals(clazz.getName(), double.class.getName())) {
					field.setDouble(obj, NumberUtils.toDouble(value.toString()));
				} else if (StringUtils.equals(clazz.getName(), Long.class.getName())
						|| StringUtils.equals(clazz.getName(), long.class.getName())) {
					field.setLong(obj, NumberUtils.toLong(value.toString()));
				} else if (StringUtils.equals(clazz.getName(), Boolean.class.getName())
						|| StringUtils.equals(clazz.getName(), boolean.class.getName())) {
					field.setBoolean(obj, BooleanUtils.toBoolean(value.toString()));
				} else if (StringUtils.equals(clazz.getName(), Float.class.getName())
						|| StringUtils.equals(clazz.getName(), float.class.getName())) {
					field.setFloat(obj, NumberUtils.toFloat(value.toString()));
				} else if (StringUtils.equals(clazz.getName(), Short.class.getName())
						|| StringUtils.equals(clazz.getName(), short.class.getName())) {
					field.setShort(obj, NumberUtils.toShort(value.toString()));
				} else {
					field.set(obj, value);
				}

				// Class declaringClass = field.getDeclaringClass();
				// @TODO
				// else if(StringUtils.equals(clazz.getName(),
				// Byte.class.getName())){
				// field.setByte(obj, Bytes.toBytes(value.toString()));
				// }
			} catch (IllegalArgumentException e) {
				logger.error(">>Exception:" + e);
			} catch (IllegalAccessException e) {
				logger.error(">>Exception:" + e);
			}
		}
	}

	/**
	 * 取得一个对像的全部属
	 * 
	 * @param clazz
	 * @return
	 */
	private static Field[] getFields(Class clazz) {
		Field[] fields = null;
		if (clazz != null) {
			fields = clazz.getDeclaredFields();
		}
		return fields;
	}

	/**
	 * 取得属性上的注解
	 * 
	 * @param field
	 * @param annotationCls
	 * @return
	 */
	private static Annotation getAnnotationOfField(Field field, Class annotationCls) {
		Annotation annotation = field.getAnnotation(annotationCls);
		return annotation;
	}

	/**
	 * 取得属性上的全部注解
	 * 
	 * @param field
	 * @return
	 */
	private static Annotation[] getAnnotationsOfField(Field field) {
		return field.getAnnotations();
	}
}
