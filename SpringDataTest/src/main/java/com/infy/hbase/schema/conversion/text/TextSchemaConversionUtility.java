/**
 * 
 */
package com.infy.hbase.schema.conversion.text;

import com.infy.hbase.schema.definition.TableSchemaDefinition;
import com.infy.hbase.schema.representation.TextRepresentation;

/**
 * @author arindam_r
 *
 */
public interface TextSchemaConversionUtility<E extends TextRepresentation> {
	public TableSchemaDefinition convertTextToSchema(E textRepresentation);
	public E convertSchemaToText(TableSchemaDefinition tableSchemaDefinition);
}
