package com.infy.hbase.schema.xstream.converter;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

public class ColumnFamilyDefinitionConverter implements Converter {

	@Override
	public boolean canConvert(Class clazz) {
		return clazz.equals(ColumnFamilyDefinition.class);
	}

	@Override
	public void marshal(Object arg0, HierarchicalStreamWriter arg1,
			MarshallingContext arg2) {
		// to be implemented
	}

	@Override
	public Object unmarshal(HierarchicalStreamReader reader,
			UnmarshallingContext context) {
		ColumnFamilyDefinition columnFamilyDefinition = new ColumnFamilyDefinition.Builder(
				reader.getAttribute("name")).build();
		while (reader.hasMoreChildren()) {
			reader.moveDown();
			if ("mapToEntity".equals(reader.getNodeName())) {
				columnFamilyDefinition.setMapToEntity(Boolean.valueOf(reader
						.getValue()));
			} else if ("tableName".equals(reader.getNodeName())) {
				columnFamilyDefinition.setTableName(reader.getValue());
			} else if ("name".equals(reader.getNodeName())) {
				columnFamilyDefinition.setName(reader.getValue());
			}
			// add un-marshalling for other nodes
			reader.moveUp();
		}
		return columnFamilyDefinition;
	}

}
