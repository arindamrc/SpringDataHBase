/**
 * 
 */
package com.infy.hbase.entity.construct.text;

import com.infy.hbase.entity.AbstractHBaseEntity;
import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.TableSchemaDefinition;
import com.infy.hbase.schema.representation.TextRepresentation;

/**
 * @author arindam_r
 *
 */
public interface TextEntityConstructionUtility<E extends TextRepresentation> {
	public E convertDefinitionToText(ColumnFamilyDefinition columnFamilyDefinition);
	public ColumnFamilyDefinition convertTextToDefinition(E textRepresentation);
}
