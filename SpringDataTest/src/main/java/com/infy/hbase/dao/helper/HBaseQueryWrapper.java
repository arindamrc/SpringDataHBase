package com.infy.hbase.dao.helper;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseQueryWrapper implements Cloneable {
	private Object rowKey;
	private String tableName;
	private String columnFamily;
	private long timeStamp;
	private Class rowKeyType;
	private Map<Object, Object> qualifierValueMap = new HashMap<Object, Object>();
	private Map<Object, Class> qualifierValueTypeMap = new HashMap<Object, Class>();
	
	private static final Logger log = LoggerFactory
	.getLogger(HBaseQueryWrapper.class);

	
	public Class getRowKeyType() {
		return rowKeyType;
	}

	public void setRowKeyType(Class rowKeyType) {
		this.rowKeyType = rowKeyType;
	}

	public Map<Object, Class> getQualifierValueTypeMap() {
		return qualifierValueTypeMap;
	}

	public void setQualifierValueTypeMap(
			Map<Object, Class> qualifierValueTypeMap) {
		this.qualifierValueTypeMap = qualifierValueTypeMap;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Map<Object, Object> getQualifierValueMap() {
		return qualifierValueMap;
	}

	public void setQualifierValueMap(Map<Object, Object> qualifierValueMap) {
		this.qualifierValueMap = qualifierValueMap;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public void addQualifierValuePair(Object key, Object value) {
		qualifierValueMap.put(key, value);
	}

	public void addQualifierValueType(Object qualifier, Class valueType) {
		qualifierValueTypeMap.put(qualifier, valueType);
	}

	public void setRowKey(Object rowKey) {
		this.rowKey = rowKey;
	}

	public Object getRowKey() {
		return rowKey;
	}

	@Override
	public HBaseQueryWrapper clone() {
		// TODO Auto-generated method stub
		HBaseQueryWrapper result = null;
		try {
			result = (HBaseQueryWrapper) super.clone();
		} catch (CloneNotSupportedException e) {
			log.debug("Cloning failed in HbaseQueryWrapper",e);
		}
		return result;
	}
}
