package com.infy.hbase.dao.support;

import java.util.Map;
import java.util.Set;

public interface HBaseDaoSupport {

public void addOrUpdateData(final String tableName, final Object rowKey,
	final String columnFamilyName,
	final Map<Object, Object> qualifierValueMap);

public void addOrUpdateData(final String tableName, final Object rowKey,
	final String columnFamilyName,
	final Map<Object, Object> qualifierValueMap, final long timestamp) ;

public Map<Object, Object> readData(final String tableName,
	final Object rowKey, final String columnFamilyName,
	final Map<Object, Class> qualifiersValueType) ;

public Map<Object, Object> readData(final String tableName,
	final Object rowKey, final String columnFamilyName,
	final Map<Object, Class> qualifiersValueType, final long timeStamp) ;

public void deleteData(final String tableName, final Object rowKey,
	final String columnFamilyName, final Set<Object> qualifiers);

public void deleteData(final String tableName, final Object rowKey,
	final String columnFamilyName,
	final Map<Object, Long> qualifierTimeStamp) ;

public void deleteData(final String tableName, final Object rowKey) ;

public void deleteData(final String tableName, final Object rowKey,
	final Long timeStamp) ;

public void deleteData(final String tableName, final Object rowKey,
	final String columnFamilyName) ;


public void deleteData(final String tableName, final Object rowKey,
	final String columnFamilyName, final Long timeStamp) ;

public Map<Object, Map<Object, Object>> readAllData(final String tableName,
	final Class rowType, final String columnFamilyName,
	final Map<Object, Class> qualifiersValueType) ;
}
