package com.infy.hbase.schema.xstream.converter;

import java.util.ArrayList;
import java.util.Collection;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.TableSchemaDefinition;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

public class TableSchemaDefinitionConverter implements Converter {

	@Override
	public boolean canConvert(Class clazz) {
		return clazz.equals(TableSchemaDefinition.class);
	}

	@Override
	public void marshal(Object object, HierarchicalStreamWriter writer,
			MarshallingContext context) {
		// implementation to be added later
	}

	@Override
	public Object unmarshal(HierarchicalStreamReader reader,
			UnmarshallingContext context) {
		TableSchemaDefinition schemaDefinition = new TableSchemaDefinition();
		schemaDefinition.setTableName(reader.getAttribute("tableName"));
		Collection<ColumnFamilyDefinition> familyDefinitions =new ArrayList<ColumnFamilyDefinition>();
		while (reader.hasMoreChildren()) {
			reader.moveDown();
			if ("columnFamily".equals(reader.getNodeName())) {
				ColumnFamilyDefinition columnFamilyDefinition = (ColumnFamilyDefinition) context
						.convertAnother(schemaDefinition,
								ColumnFamilyDefinition.class);
				familyDefinitions.add(columnFamilyDefinition);
				reader.moveUp();
			}
		}
		schemaDefinition.setColumnFamilies(familyDefinitions);
		return schemaDefinition;
	}

}
