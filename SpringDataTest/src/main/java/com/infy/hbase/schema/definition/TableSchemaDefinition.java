package com.infy.hbase.schema.definition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TableSchemaDefinition {
	private String name;
	private String description;
	private String tableName;
//	private Map<String, ColumnFamilyDefinition> columns = new HashMap<String, ColumnFamilyDefinition>();
	private Collection<ColumnFamilyDefinition> columnFamilies;
	

	public Collection<ColumnFamilyDefinition> getColumnFamilies() {
		return columnFamilies;
	}

	public void setColumnFamilies(Collection<ColumnFamilyDefinition> columnFamilies) {
		this.columnFamilies = columnFamilies;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/*public void addColumn(ColumnFamilyDefinition column) {
		columns.put(column.getName(), column);
	}

	public Collection<ColumnFamilyDefinition> getColumns() {
		return columns.values();
	}

	public ColumnFamilyDefinition getColumnDefinition(String name) {
		return columns.get(name);
	}*/
}
