package com.infy.hbase.entity.construct.text;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.QualifierDefinition;
import com.infy.hbase.schema.representation.XMLDOMTextRepresentation;
import com.infy.hbase.utilities.XMLDOMUtilities;
import com.thoughtworks.xstream.XStream;

public class XMLDOMEntityConstructionUtility implements
		TextEntityConstructionUtility<XMLDOMTextRepresentation> {

	private Logger logger = LoggerFactory
			.getLogger(XMLDOMEntityConstructionUtility.class);

	@Override
	public XMLDOMTextRepresentation convertDefinitionToText(
			ColumnFamilyDefinition columnFamilyDefinition) {
		XMLDOMTextRepresentation representation = new XMLDOMTextRepresentation();

		XStream xStream = new XStream();

		xStream.alias("hbaseEntity", ColumnFamilyDefinition.class);
		xStream.alias("qualifier", QualifierDefinition.class);
		xStream.useAttributeFor(QualifierDefinition.class, "type");

		String xmlString = xStream.toXML(columnFamilyDefinition);

		try {
			representation.setRepresentation(XMLDOMUtilities
					.loadXMLFrom(xmlString));
		} catch (SAXException e) {
			logger.error("Error while parsing XML Doc", e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("Error in stream operations", e);
			e.printStackTrace();
		}

		return representation;
	}

	@Override
	public ColumnFamilyDefinition convertTextToDefinition(
			XMLDOMTextRepresentation textRepresentation) {
		Document document = textRepresentation.getRepresentation();
		String xmlString = XMLDOMUtilities.loadStringFromDoc(document);
		XStream xStream = new XStream();

		xStream.alias("hbaseEntity", ColumnFamilyDefinition.class);
		xStream.alias("qualifier", QualifierDefinition.class);
		xStream.useAttributeFor(QualifierDefinition.class, "type");

		ColumnFamilyDefinition columnFamilyDefinition = (ColumnFamilyDefinition) xStream
				.fromXML(xmlString);
		return columnFamilyDefinition;
	}

}
