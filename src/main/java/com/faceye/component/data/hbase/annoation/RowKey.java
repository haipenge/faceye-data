package com.faceye.component.data.hbase.annoation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * RowKey 注解,as hbase row key 
 * @author songhaipeng
 *
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = {ElementType.FIELD })
public @interface RowKey {

}
