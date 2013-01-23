package com.infy.hbase.schema.definition;

public class QualifierDefinition {
	private String name;
	private String type;
	
	public QualifierDefinition() {
		// TODO Auto-generated constructor stub
	}
	
	public QualifierDefinition(String name) {
		this.name=name;
	}
	
	public QualifierDefinition(String name,String type) {
		this.name=name;
		this.type=type;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
}
