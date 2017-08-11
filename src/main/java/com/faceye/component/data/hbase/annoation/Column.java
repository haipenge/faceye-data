package com.faceye.component.data.hbase.annoation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HBase列注解
 * 
 * @author songhaipeng
 *
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.METHOD, ElementType.FIELD })
public @interface Column {

	/**
	 * 定义列族
	 * 
	 * @return
	 */
	public String family() default "_default_family_";

	/**
	 * 列修饰符
	 * 
	 * @return
	 */
	public String value();

}
