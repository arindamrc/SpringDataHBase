package com.infy.hbase.dao;

import java.util.List;
import java.util.Map;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;

public class TableRowDescriptorImpl implements TableRowDescriptor {
	
	private Object rowKey;
	private String tableName;
	private String tableDescription;
	private Map<String, ColumnFamilyDefinition> columnFamilies;

	@Override
	public void setRowKey(Object rowKey) {
		this.rowKey=rowKey;
	}

	@Override
	public Object getRowKey() {
		return rowKey;
	}

	@Override
	public void setTableName(String tableName) {
		this.tableName=tableName;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public void setTableDescription(String description) {
		this.tableDescription=tableDescription;
	}

	@Override
	public String getTableDescription() {
		return tableDescription;
	}

	@Override
	public void addColumnFamilyDefinition(
			ColumnFamilyDefinition columnFamilyDefinition) {
		columnFamilies.put(columnFamilyDefinition.getName(), columnFamilyDefinition);
	}

	@Override
	public ColumnFamilyDefinition getColumnFamilyDefinition(String columnfamilyName) {
		return columnFamilies.get(columnfamilyName);
	}

	@Override
	public List<ColumnFamilyDefinition> getColumnFamilies() {
		return (List<ColumnFamilyDefinition>)columnFamilies.values();
	}

	@Override
	public void setColumnFamilies(
			Map<String, ColumnFamilyDefinition> columnFamilyMap) {
		this.columnFamilies=columnFamilyMap;
	}

}
