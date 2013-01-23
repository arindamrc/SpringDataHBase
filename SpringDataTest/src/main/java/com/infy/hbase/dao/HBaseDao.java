package com.infy.hbase.dao;

import java.util.List;

import com.infy.hbase.dao.helper.HBaseQueryWrapper;

/**
 * @author arindam_r
 *
 */
public interface HBaseDao{
	
	boolean create(HBaseQueryWrapper hBaseQuery);
	
	HBaseQueryWrapper read(HBaseQueryWrapper hBaseQuery);
	
	List<HBaseQueryWrapper> readAll(HBaseQueryWrapper hBaseQuery);
	
	boolean update(HBaseQueryWrapper hBaseQuery);
	
	boolean updateAll(List<HBaseQueryWrapper> hBaseQueries);
	
	void delete(HBaseQueryWrapper hBaseQuery);
	
	void deleteAll(List<HBaseQueryWrapper> hBaseQueries);
	
	

	/**
	 * @param columnFamilyDefinition
	 * @param qualifier
	 * @param value
	 * @return
	 *//*
	boolean create(ColumnFamilyDefinition columnFamilyDefinition,
			Object qualifier, Object value);
	
	*//**
	 * @param columnFamilyDefinition
	 * @return
	 *//*
	boolean create(ColumnFamilyDefinition columnFamilyDefinition);

	*//**
	 * @param columnFamilyDefinition
	 * @param qualifier
	 * @return
	 *//*
	Object readLatest(ColumnFamilyDefinition columnFamilyDefinition,
			Object qualifier);

	*//**
	 * @param columnFamilyDefinition
	 * @return
	 *//*
	Map<Object, Object> readLatestAll(
			ColumnFamilyDefinition columnFamilyDefinition);

	*//**
	 * @param columnFamilyDefinition
	 * @param qualifier
	 * @return
	 *//*
	boolean update(ColumnFamilyDefinition columnFamilyDefinition,
			Object qualifier);

	*//**
	 * @param columnFamilyDefinition
	 * @param qualifier
	 * @return
	 *//*
	boolean deleteLatest(ColumnFamilyDefinition columnFamilyDefinition,
			Object qualifier);

	*//**
	 * @param columnFamilyDefinition
	 * @return
	 *//*
	boolean deleteLatestAll(ColumnFamilyDefinition columnFamilyDefinition);
	
	public TableRowDescriptor getTableRowDescriptor();
	
	public void setTableRowDescriptor(TableRowDescriptor rowDescriptor);*/
}
