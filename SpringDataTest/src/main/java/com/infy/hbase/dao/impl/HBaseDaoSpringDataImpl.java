package com.infy.hbase.dao.impl;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infy.hbase.dao.HBaseGenericDao;
import com.infy.hbase.dao.helper.HBaseAnnotationReader;
import com.infy.hbase.dao.helper.HBaseQueryWrapper;
import com.infy.hbase.dao.support.HBaseDaoSupport;
import com.infy.hbase.dao.support.SpringDataDaoSupport;
import com.infy.hbase.entity.AbstractHBaseEntity;
import com.infy.hbase.entity.annotations.HBaseColumnFamily;
import com.infy.hbase.entity.annotations.HBaseQualifierName;
import com.infy.hbase.entity.annotations.HBaseTable;
import com.infy.hbase.exceptions.HBaseAnnotationException;

/**
 * @author arindam_r
 *
 * @param <T>
 * @param <RK>
 */
public class HBaseDaoSpringDataImpl<T extends AbstractHBaseEntity, RK extends Object>
		implements HBaseGenericDao<T, RK> {

	private HBaseDaoSupport daoSupport;

	private final Class<T> entityType;

	private final Class<RK> rowKeyType;

	private static final Logger log = LoggerFactory
			.getLogger(SpringDataDaoSupport.class);

	private String tableName;
	private String columnFamilyName;
	private Map<String, Field> fieldQualifierMap;

	/**
	 * @param entityType
	 * @param rowKeyType
	 */
	public HBaseDaoSpringDataImpl(Class<T> entityType, Class<RK> rowKeyType) {
		this.entityType = entityType;
		this.rowKeyType = rowKeyType;
		initialize();
	}

	/**
	 * 
	 */
	/**
	 * 
	 * TODO
	 */
	public HBaseDaoSpringDataImpl() {
		entityType = null;
		rowKeyType = null;
	}
	
	/**
	 * @return
	 */
	/**
	 * @return
	 * SpringDataDaoSupport
	 * TODO
	 */
	public HBaseDaoSupport getDaoSupport() {
		return daoSupport;
	}

	/**
	 * @param daoSupport
	 */
	/**
	 * @param daoSupport
	 * void
	 * TODO
	 */
	public void setDaoSupport(HBaseDaoSupport daoSupport) {
		this.daoSupport = daoSupport;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#create(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#create(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	@Override
	public boolean create(HBaseQueryWrapper hBaseQuery) {
		if (hBaseQuery.getTimeStamp() == 0) {
			daoSupport.addOrUpdateData(hBaseQuery.getTableName(),
					hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueMap());
		} else {
			daoSupport.addOrUpdateData(hBaseQuery.getTableName(),
					hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueMap(),
					hBaseQuery.getTimeStamp());
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#read(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#read(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	@Override
	public HBaseQueryWrapper read(HBaseQueryWrapper hBaseQuery) {
		HBaseQueryWrapper result = new HBaseQueryWrapper();
		result = hBaseQuery.clone();
		if (hBaseQuery.getTimeStamp() == 0) {
			result.setQualifierValueMap(daoSupport.readData(
					hBaseQuery.getTableName(), hBaseQuery.getRowKey(),
					hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueTypeMap()));
		} else {
			result.setQualifierValueMap(daoSupport.readData(
					hBaseQuery.getTableName(), hBaseQuery.getRowKey(),
					hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueTypeMap(),
					hBaseQuery.getTimeStamp()));
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#readAll(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#readAll(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	@Override
	public List<HBaseQueryWrapper> readAll(HBaseQueryWrapper hBaseQuery) {
		List<HBaseQueryWrapper> hBaseQueryWrappers = new ArrayList<HBaseQueryWrapper>();
		Map<Object, Map<Object, Object>> result = daoSupport.readAllData(
				hBaseQuery.getTableName(), hBaseQuery.getRowKeyType(),
				hBaseQuery.getColumnFamily(),
				hBaseQuery.getQualifierValueTypeMap());
		for (Map.Entry<Object, Map<Object, Object>> rowEntry : result
				.entrySet()) {
			HBaseQueryWrapper wrapper = new HBaseQueryWrapper();
			wrapper.setRowKey(rowEntry.getKey());
			wrapper.setTableName(hBaseQuery.getTableName());
			wrapper.setColumnFamily(hBaseQuery.getColumnFamily());
			wrapper.setQualifierValueMap(rowEntry.getValue());
			hBaseQueryWrappers.add(wrapper);
		}
		return hBaseQueryWrappers;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#update(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#update(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	@Override
	public boolean update(HBaseQueryWrapper hBaseQuery) {
		if (hBaseQuery.getTimeStamp() == 0) {
			daoSupport.addOrUpdateData(hBaseQuery.getTableName(),
					hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueMap());
		} else {
			daoSupport.addOrUpdateData(hBaseQuery.getTableName(),
					hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueMap(),
					hBaseQuery.getTimeStamp());
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#updateAll(java.util.List)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#updateAll(java.util.List)
	 */
	@Override
	public boolean updateAll(List<HBaseQueryWrapper> hBaseQueries) {
		for (HBaseQueryWrapper queryWrapper : hBaseQueries) {
			update(queryWrapper);
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#delete(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#delete(com.infy.hbase.dao.helper.HBaseQueryWrapper)
	 */
	@Override
	public void delete(HBaseQueryWrapper hBaseQuery) {
		if (hBaseQuery.getColumnFamily() != null
				&& hBaseQuery.getQualifierValueMap() != null) {// delete
																// specific
																// qualifiers
			daoSupport.deleteData(hBaseQuery.getTableName(),
					hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily(),
					hBaseQuery.getQualifierValueMap().keySet());
		} else if (hBaseQuery.getColumnFamily() != null) {// delete specific
															// column family
			if (hBaseQuery.getTimeStamp() != 0) {// consider timestamp
				daoSupport.deleteData(hBaseQuery.getTableName(),
						hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily(),
						hBaseQuery.getTimeStamp());
			} else {
				daoSupport.deleteData(hBaseQuery.getTableName(),
						hBaseQuery.getRowKey(), hBaseQuery.getColumnFamily());
			}
		} else {// delete entire row
			if (hBaseQuery.getTimeStamp() != 0) {// consider timestamp
				daoSupport.deleteData(hBaseQuery.getTableName(),
						hBaseQuery.getRowKey(), hBaseQuery.getTimeStamp());
			} else {
				daoSupport.deleteData(hBaseQuery.getTableName(),
						hBaseQuery.getRowKey());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#deleteAll(java.util.List)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseDao#deleteAll(java.util.List)
	 */
	@Override
	public void deleteAll(List<HBaseQueryWrapper> hBaseQueries) {
		for (HBaseQueryWrapper queryWrapper : hBaseQueries) {
			delete(queryWrapper);
		}
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#read(java.lang.Object)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#read(java.lang.Object)
	 */
	@Override
	public T read(RK id) {
		T entity = null;
		try {
			Map<Object, Class> qualifierValueTypeMap = new HashMap<Object, Class>();
			for (Map.Entry<String, Field> entry : fieldQualifierMap.entrySet()) {
				qualifierValueTypeMap.put(entry.getKey(), entry.getValue()
						.getType());
			}

			entity = entityType.newInstance();
			PropertyUtils.setProperty(entity, "rowKey", id);
			Map<Object, Object> qualifierValueMap = daoSupport.readData(
					tableName, id, columnFamilyName, qualifierValueTypeMap);
			for (Map.Entry<Object, Object> entry : qualifierValueMap.entrySet()) {
				PropertyUtils.setProperty(entity,
						fieldQualifierMap.get(entry.getKey().toString())
								.getName(), entry.getValue());
			}
		} catch (InstantiationException e) {
			log.debug("T read(RK id): ", e);
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			log.debug("T read(RK id): ", e);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			log.debug("T read(RK id): ", e);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			log.debug("T read(RK id): ", e);
			e.printStackTrace();
		}
		catch (Exception e) {
			log.debug("T read(RK id): ", e);
			e.printStackTrace();
		}

		return entity;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#readAll()
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#readAll()
	 */
	@Override
	public List<T> readAll() {
		List<T> entities = new ArrayList<T>();
		try {
			Map<Object, Class> qualifierValueTypeMap = new HashMap<Object, Class>();
			for (Map.Entry<String, Field> entry : fieldQualifierMap.entrySet()) {
				qualifierValueTypeMap.put(entry.getKey(), entry.getValue()
						.getType());
			}
			
			Map<Object, Map<Object, Object>> result = daoSupport.readAllData(
					tableName, rowKeyType, columnFamilyName,
					qualifierValueTypeMap);
			T entity = null;
			for (Map.Entry<Object, Map<Object, Object>> rowEntry : result
					.entrySet()) {
				entity = entityType.newInstance();
				PropertyUtils.setProperty(entity, "rowKey", rowEntry.getKey());
				for (Map.Entry<Object, Object> colFamEntry : rowEntry
						.getValue().entrySet()) {
					PropertyUtils.setProperty(
							entity,
							fieldQualifierMap.get(
									colFamEntry.getKey().toString()).getName(),
							colFamEntry.getValue());
				}
				entities.add(entity);
			}
		} catch (InstantiationException e) {
			log.debug("List<T> readAll(): ", e);
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			log.debug("List<T> readAll(): ", e);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			log.debug("List<T> readAll(): ", e);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			log.debug("List<T> readAll(): ", e);
			e.printStackTrace();
		}
		return entities;
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#update(com.infy.hbase.entity.AbstractHBaseEntity)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#update(com.infy.hbase.entity.AbstractHBaseEntity)
	 */
	@Override
	public void update(T transientObject) {
		Map<Object, Object> qualifierValueMap = new HashMap<Object, Object>();
		try {
			for (Map.Entry<String, Field> entry : fieldQualifierMap.entrySet()) {
				qualifierValueMap.put(entry.getKey(), PropertyUtils.getProperty(
						transientObject, entry.getValue().getName()));
			}
			daoSupport.addOrUpdateData(tableName, transientObject.getRowKey(),
					columnFamilyName, qualifierValueMap);
		} catch (IllegalAccessException e) {
			log.debug("void update(T transientObject): ", e);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			log.debug("void update(T transientObject): ", e);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			log.debug("void update(T transientObject): ", e);
			e.printStackTrace();
		}catch (Exception e) {
			log.debug("void update(T transientObject): ", e);
			e.printStackTrace();
		}

	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#delete(com.infy.hbase.entity.AbstractHBaseEntity)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#delete(com.infy.hbase.entity.AbstractHBaseEntity)
	 */
	@Override
	public void delete(T persistentObject) {
		try {
			Set<String> temp=fieldQualifierMap.keySet();
			Set<Object> qualifiers=new TreeSet<Object>(temp);
			/*for(String qualifier: temp){
				qualifiers.add(qualifier);
			}*/
			
			daoSupport.deleteData(tableName,
					persistentObject.getRowKey(), columnFamilyName,
					qualifiers);
		} catch (Exception e) {
			log.debug("void delete(T persistentObject): ", e);
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#updateCollection(java.util.Collection)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#updateCollection(java.util.Collection)
	 */
	@Override
	public void updateCollection(Collection<T> o) {
		for(T entity: o){
			update(entity);
		}

	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#deleteCollection(java.util.Collection)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#deleteCollection(java.util.Collection)
	 */
	@Override
	public void deleteCollection(Collection<T> o) {
		for(T entity: o){
			delete(entity);
		}
	}

	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#create(com.infy.hbase.entity.AbstractHBaseEntity)
	 */
	/* (non-Javadoc)
	 * @see com.infy.hbase.dao.HBaseGenericDao#create(com.infy.hbase.entity.AbstractHBaseEntity)
	 */
	@Override
	public boolean create(T newInstance) {
		Map<Object, Object> qualifierValueMap = new HashMap<Object, Object>();
		try {
			for (Map.Entry<String, Field> entry : fieldQualifierMap.entrySet()) {
				qualifierValueMap.put(entry.getKey(), PropertyUtils.getProperty(
						newInstance, entry.getValue().getName()));
			}
			daoSupport.addOrUpdateData(tableName, newInstance.getRowKey(),
					columnFamilyName, qualifierValueMap);
		} catch (IllegalAccessException e) {
			log.debug("boolean create(T newInstance): ", e);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			log.debug("boolean create(T newInstance): ", e);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			log.debug("boolean create(T newInstance): ", e);
			e.printStackTrace();
		}catch (Exception e) {
			log.debug("boolean create(T newInstance): ", e);
			e.printStackTrace();
		}
		
		return true;
	}

	private void initialize() {
		try {
			tableName = HBaseAnnotationReader.readClassAnnotationValue(
					entityType, HBaseTable.class);
			columnFamilyName = HBaseAnnotationReader.readClassAnnotationValue(
					entityType, HBaseColumnFamily.class);
			fieldQualifierMap = HBaseAnnotationReader.readFieldAnnotationValue(
					entityType, HBaseQualifierName.class);
		} catch (HBaseAnnotationException e) {
			log.debug("void initialize(): ", e);
			e.printStackTrace();
		}
	}
}