package com.infy.hbase.schema.conversion.text;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.QualifierDefinition;
import com.infy.hbase.schema.definition.TableSchemaDefinition;
import com.infy.hbase.schema.representation.XMLDOMTextRepresentation;
import com.infy.hbase.schema.xstream.converter.ColumnFamilyDefinitionConverter;
import com.infy.hbase.schema.xstream.converter.TableSchemaDefinitionConverter;
import com.infy.hbase.utilities.XMLDOMUtilities;
import com.thoughtworks.xstream.XStream;

public class XMLDOMSchemaConversionUtility implements
		TextSchemaConversionUtility<XMLDOMTextRepresentation> {

	private Logger logger = LoggerFactory
			.getLogger(XMLDOMSchemaConversionUtility.class);

	@Override
	public TableSchemaDefinition convertTextToSchema(
			XMLDOMTextRepresentation textRepresentation) {
		Document document = textRepresentation.getRepresentation();
		String xmlString = XMLDOMUtilities.loadStringFromDoc(document);
		XStream xStream = new XStream();
		
		xStream.alias("table", TableSchemaDefinition.class);
		xStream.alias("columnFamily", ColumnFamilyDefinition.class);
		xStream.alias("qualifier", QualifierDefinition.class);
		xStream.useAttributeFor(TableSchemaDefinition.class, "tableName");
		xStream.useAttributeFor(ColumnFamilyDefinition.class, "name");
		xStream.useAttributeFor(QualifierDefinition.class, "type");
		xStream.useAttributeFor(ColumnFamilyDefinition.class, "mapToEntity");
		
		xStream.registerConverter(new TableSchemaDefinitionConverter());
		xStream.registerConverter(new ColumnFamilyDefinitionConverter());
		TableSchemaDefinition tableSchemaDefinition = (TableSchemaDefinition) xStream
				.fromXML(xmlString);
		return tableSchemaDefinition;
	}

	@Override
	public XMLDOMTextRepresentation convertSchemaToText(
			TableSchemaDefinition tableSchemaDefinition) {
		XMLDOMTextRepresentation representation = new XMLDOMTextRepresentation();

		XStream xStream = new XStream();

		xStream.alias("table", TableSchemaDefinition.class);
		xStream.alias("columnFamily", ColumnFamilyDefinition.class);
		xStream.alias("qualifier", QualifierDefinition.class);
		xStream.useAttributeFor(TableSchemaDefinition.class, "tableName");
		xStream.useAttributeFor(ColumnFamilyDefinition.class, "name");
		xStream.useAttributeFor(QualifierDefinition.class, "type");
		xStream.useAttributeFor(ColumnFamilyDefinition.class, "mapToEntity");

		String xmlString = xStream.toXML(tableSchemaDefinition);

		try {
			representation.setRepresentation(XMLDOMUtilities.loadXMLFrom(xmlString));
		} catch (SAXException e) {
			logger.error("Error while parsing XML Doc", e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("Error in stream operations", e);
			e.printStackTrace();
		}

		return representation;
	}
}
