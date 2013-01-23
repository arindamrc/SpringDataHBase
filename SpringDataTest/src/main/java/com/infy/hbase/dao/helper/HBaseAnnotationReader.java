package com.infy.hbase.dao.helper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.infy.hbase.entity.annotations.HBaseColumnFamily;
import com.infy.hbase.entity.annotations.HBaseQualifierName;
import com.infy.hbase.entity.annotations.HBaseTable;
import com.infy.hbase.exceptions.HBaseAnnotationException;

public class HBaseAnnotationReader {
	public static String readClassAnnotationValue(Class entityClass,
			Class annotationClass) throws HBaseAnnotationException {
		Annotation annotation = entityClass.getAnnotation(annotationClass);
		if (annotation instanceof HBaseTable) {
			HBaseTable table = (HBaseTable) annotation;
			return table.name();
		} else if (annotation instanceof HBaseColumnFamily) {
			HBaseColumnFamily columnFamily = (HBaseColumnFamily) annotation;
			return columnFamily.name();
		} else {
			throw new HBaseAnnotationException("No Such Class Annotation");
		}
	}

	public static Map<String, Field> readFieldAnnotationValue(Class entityClass,
			Class annotationClass) throws HBaseAnnotationException {
		Field[] fields = entityClass.getDeclaredFields();
		Map<String, Field> annoValueMap = new HashMap<String, Field>();
		for (Field field : fields) {
			Annotation annotation = field.getAnnotation(annotationClass);
			if (annotation != null) {
				if (annotation instanceof HBaseQualifierName) {
					HBaseQualifierName qualifierName = (HBaseQualifierName) annotation;
					annoValueMap.put(qualifierName.value(), field);
				} else {
					throw new HBaseAnnotationException(
							"No such field annotation");
				}
			}
		}
		return annoValueMap;
	}

}
