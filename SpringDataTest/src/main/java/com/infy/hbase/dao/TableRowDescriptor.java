/**
 * 
 */
package com.infy.hbase.dao;

import java.util.List;
import java.util.Map;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;

/**
 * @author arindam_r
 * 
 */
public interface TableRowDescriptor {

	/**
	 * @param rowKey
	 */
	public void setRowKey(Object rowKey);

	/**
	 * @return
	 */
	public Object getRowKey();

	/**
	 * @param tableName
	 */
	public void setTableName(String tableName);

	/**
	 * @return
	 */
	public String getTableName();

	/**
	 * @param description
	 */
	public void setTableDescription(String description);

	/**
	 * @return
	 */
	public String getTableDescription();

	/**
	 * @param columnFamilyDefinition
	 */
	public void addColumnFamilyDefinition(
			ColumnFamilyDefinition columnFamilyDefinition);

	/**
	 * @param columnfamiliName
	 * @return
	 */
	public ColumnFamilyDefinition getColumnFamilyDefinition(String columnfamiliName);

	/**
	 * @return
	 */
	public List<ColumnFamilyDefinition> getColumnFamilies();

	/**
	 * @param columnFamilyMap
	 */
	public void setColumnFamilies(
			Map<String, ColumnFamilyDefinition> columnFamilyMap);
}
