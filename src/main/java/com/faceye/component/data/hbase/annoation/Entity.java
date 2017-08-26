package com.faceye.component.data.hbase.annoation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HBase实体注解
 * @author songhaipeng
 *
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.TYPE })
public @interface Entity {
	/**
	 * as hbase table name
	 * @return
	 */
	public String value();
}
