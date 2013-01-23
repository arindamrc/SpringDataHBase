package com.infy.hbase.utilities;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteArrayConversionUtilities {
	public static byte[] getByteArray(Object object) {
		byte[] returnValue = null;
		if (object instanceof Integer) {
			returnValue = Bytes.toBytes(Integer.class.cast(object));
		} else if (object instanceof Boolean) {
			returnValue = Bytes.toBytes(Boolean.class.cast(object));
		} else if (object instanceof Double) {
			returnValue = Bytes.toBytes(Double.class.cast(object));
		} else if (object instanceof Long) {
			returnValue = Bytes.toBytes(Long.class.cast(object));
		} else if (object instanceof Short) {
			returnValue = Bytes.toBytes(Short.class.cast(object));
		} else if (object instanceof Float) {
			returnValue = Bytes.toBytes(Float.class.cast(object));
		} else if (object instanceof String) {
			returnValue = Bytes.toBytes(String.class.cast(object));
		}
		return returnValue;
	}

	public static Object getObject(byte[] byteArray, Class type) {
		if (byteArray == null || type == null) {
			throw new NullPointerException();
		}
		Object returnValue = null;
		String value = Bytes.toString(byteArray);
		if (type.getName().equals(Integer.class.getName())) {
			returnValue = Integer.parseInt(value);
		} else if (type.getName().equals(Boolean.class.getName())) {
			returnValue = Boolean.parseBoolean(value);
		} else if (type.getName().equals(Short.class.getName())) {
			returnValue = Short.parseShort(value);
		} else if (type.getName().equals(Double.class.getName())) {
			returnValue = Double.parseDouble(value);
		} else if (type.getName().equals(Long.class.getName())) {
			returnValue = Long.parseLong(value);
		} else if (type.getName().equals(Float.class.getName())) {
			returnValue = Float.parseFloat(value);
		} else if (type.getName().equals(String.class.getName())) {
			returnValue = value;
		}
		/*
		 * if (type.getName().equals(Integer.class.getName())) { returnValue =
		 * Bytes.toInt(byteArray); } else if
		 * (type.getName().equals(Boolean.class.getName())) { returnValue =
		 * Bytes.toBoolean(byteArray); } else if
		 * (type.getName().equals(Short.class.getName())) { returnValue =
		 * Bytes.toShort(byteArray); } else if
		 * (type.getName().equals(Double.class.getName())) { returnValue =
		 * Bytes.toDouble(byteArray); } else if
		 * (type.getName().equals(Long.class.getName())) { returnValue =
		 * Bytes.toLong(byteArray); } else if
		 * (type.getName().equals(Float.class.getName())) { returnValue =
		 * Bytes.toFloat(byteArray); } else if
		 * (type.getName().equals(String.class.getName())) { returnValue =
		 * Bytes.toString(byteArray); }
		 */
		return returnValue;
	}
}
