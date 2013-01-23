package com.infy.hbase.schema.representation;

public interface TextRepresentation<T> {
	public void setRepresentation(T represenation);
	public T getRepresentation();
}
