package com.infy.hbase.schema.representation;
import org.w3c.dom.Document;

public class XMLDOMTextRepresentation implements TextRepresentation<Document> {
	
	private Document document;

	@Override
	public void setRepresentation(Document represenation) {
		this.document=represenation;
	}

	@Override
	public Document getRepresentation() {
		return document;
	}

}
