package com.infy.hbase.dao.support;

import static com.infy.hbase.utilities.ByteArrayConversionUtilities.getByteArray;
import static com.infy.hbase.utilities.ByteArrayConversionUtilities.getObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

import com.infy.hbase.utilities.HBaseTemplate;

public class AhamDaoSupport implements HBaseDaoSupport {

	private static final Logger log = LoggerFactory
			.getLogger(AhamDaoSupport.class);

	private HBaseTemplate ahamHBaseTemplate;

	public void setAhamHBaseTemplate(HBaseTemplate ahamHBaseTemplate) {
		this.ahamHBaseTemplate = ahamHBaseTemplate;
	}

	@Override
	public void addOrUpdateData(String tableName, Object rowKey,
			String columnFamilyName, Map<Object, Object> qualifierValueMap) {
		try {
			Put p = new Put(getByteArray(rowKey));
			for (Map.Entry<Object, Object> entry : qualifierValueMap.entrySet()) {
				p.add(getByteArray(columnFamilyName),
						getByteArray(entry.getKey()),
						getByteArray(entry.getValue()));
			}
			ahamHBaseTemplate.put(tableName, p);
		} catch (IOException e) {
			e.printStackTrace();
			log.debug(
					" void addOrUpdateData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Object> qualifierValueMap): ",
					e);
		}

	}

	@Override
	public void addOrUpdateData(String tableName, Object rowKey,
			String columnFamilyName, Map<Object, Object> qualifierValueMap,
			long timestamp) {
		try {
			Put p = new Put(getByteArray(rowKey));
			for (Map.Entry<Object, Object> entry : qualifierValueMap.entrySet()) {
				p.add(getByteArray(columnFamilyName),
						getByteArray(entry.getKey()), timestamp,
						getByteArray(entry.getValue()));
			}
			ahamHBaseTemplate.put(tableName, p);
		} catch (IOException e) {
			e.printStackTrace();
			log.debug(
					"void addOrUpdateData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Object> qualifierValueMap,long timestamp): ",
					e);
		}
	}

	@Override
	public Map<Object, Object> readData(String tableName, Object rowKey,
			String columnFamilyName, Map<Object, Class> qualifiersValueType) {
		Map<Object, Object> tempQualifierValueMap = new HashMap<Object, Object>();
		try {
			Get get = new Get(getByteArray(rowKey));
			Result result = ahamHBaseTemplate.get(tableName, get);
			byte[] valueByte;
			for (Map.Entry<Object, Class> entry : qualifiersValueType
					.entrySet()) {
				valueByte = result.getValue(Bytes.toBytes(columnFamilyName),
						getByteArray(entry.getKey()));
				if (valueByte != null) {
					tempQualifierValueMap.put(entry.getKey(),
							getObject(valueByte, entry.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.debug(
					"Map<Object, Object> readData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Class> qualifiersValueType)",
					e);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"Map<Object, Object> readData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Class> qualifiersValueType)",
					e);
		}
		return tempQualifierValueMap;
	}

	@Override
	public Map<Object, Object> readData(String tableName, Object rowKey,
			String columnFamilyName, Map<Object, Class> qualifiersValueType,
			long timeStamp) {
		Map<Object, Object> tempQualifierValueMap = new HashMap<Object, Object>();
		try {
			Get get = new Get(getByteArray(rowKey));
			get.setTimeStamp(timeStamp);
			Result result = ahamHBaseTemplate.get(tableName, get);
			byte[] valueByte;
			for (Map.Entry<Object, Class> entry : qualifiersValueType
					.entrySet()) {
				valueByte = result.getValue(Bytes.toBytes(columnFamilyName),
						getByteArray(entry.getKey()));
				if (valueByte != null) {
					tempQualifierValueMap.put(entry.getKey(),
							getObject(valueByte, entry.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.debug(
					"Map<Object, Object> readData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Class> qualifiersValueType,long timeStamp)",
					e);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"Map<Object, Object> readData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Class> qualifiersValueType,long timeStamp)",
					e);
		}
		return tempQualifierValueMap;
	}

	@Override
	public void deleteData(String tableName, Object rowKey,
			String columnFamilyName, Set<Object> qualifiers) {
		try {
			Delete delete = new Delete(getByteArray(rowKey));
			for (Object qualifier : qualifiers) {
				delete.deleteColumn(Bytes.toBytes(columnFamilyName),
						getByteArray(qualifier));
			}
			ahamHBaseTemplate.delete(tableName, delete);
		} catch (IOException e) {
			e.printStackTrace();
			log.debug(
					"void deleteData(String tableName, Object rowKey,String columnFamilyName, Set<Object> qualifiers): ",
					e);
		}
	}

	@Override
	public void deleteData(String tableName, Object rowKey,
			String columnFamilyName, Map<Object, Long> qualifierTimeStamp) {
		try {
			Delete delete = new Delete(getByteArray(rowKey));
			for (Map.Entry<Object, Long> entry : qualifierTimeStamp.entrySet()) {
				delete.deleteColumn(Bytes.toBytes(columnFamilyName),
						getByteArray(entry.getKey()), entry.getValue());
			}
			ahamHBaseTemplate.delete(tableName, delete);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"void deleteData(String tableName, Object rowKey,String columnFamilyName, Map<Object, Long> qualifierTimeStamp): ",
					e);
		}
	}

	@Override
	public void deleteData(String tableName, Object rowKey) {
		try {
			Delete delete = new Delete(getByteArray(rowKey));
			ahamHBaseTemplate.delete(tableName, delete);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug("void deleteData(String tableName, Object rowKey) : ", e);
		}
	}

	@Override
	public void deleteData(String tableName, Object rowKey, Long timeStamp) {
		try {
			Delete delete = new Delete(getByteArray(rowKey));
			delete.setTimestamp(timeStamp);
			ahamHBaseTemplate.delete(tableName, delete);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"void deleteData(String tableName, Object rowKey, Long timeStamp): ",
					e);
		}

	}

	@Override
	public void deleteData(String tableName, Object rowKey,
			String columnFamilyName) {
		try {
			Delete delete = new Delete(getByteArray(rowKey));
			delete.deleteFamily(Bytes.toBytes(columnFamilyName));
			ahamHBaseTemplate.delete(tableName, delete);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"void deleteData(String tableName, Object rowKey,String columnFamilyName): ",
					e);
		}
	}

	@Override
	public void deleteData(String tableName, Object rowKey,
			String columnFamilyName, Long timeStamp) {
		try {
			Delete delete = new Delete(getByteArray(rowKey));
			delete.deleteFamily(Bytes.toBytes(columnFamilyName), timeStamp);
			ahamHBaseTemplate.delete(tableName, delete);
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"void deleteData(String tableName, Object rowKey,String columnFamilyName, Long timeStamp): ",
					e);
		}
	}

	@Override
	public Map<Object, Map<Object, Object>> readAllData(String tableName,
			Class rowType, String columnFamilyName,
			Map<Object, Class> qualifiersValueType) {
		Map<Object, Map<Object, Object>> rowFamily = new HashMap<Object, Map<Object, Object>>();
		ResultScanner resultScanner = null;
		Scan scan = new Scan();
		try {
			resultScanner = HBaseTemplate.getInstance().getScanner(tableName,
					scan);
			for (Result result : resultScanner) {
				Map<Object, Object> qualifierValueMap = null;
				qualifierValueMap = new HashMap<Object, Object>();
				for (Map.Entry<Object, Class> entry : qualifiersValueType
						.entrySet()) {
					Object value = null;
					try {
						value = getObject(result.getValue(
								Bytes.toBytes(columnFamilyName),
								getByteArray(entry.getKey())), entry.getValue());
					} catch (NullPointerException e) {
						log.error("No such key in family: " + entry.getKey()
								+ " " + columnFamilyName, e);
					} catch (NumberFormatException e) {
						log.error(
								"value type mismatch for key: "
										+ entry.getKey() + " "
										+ entry.getValue(), e);
					}
					if (value != null) {
						qualifierValueMap.put(entry.getKey(), value);
					}
				}
				rowFamily.put(getObject(result.getRow(), rowType),
						qualifierValueMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(
					"Map<Object, Map<Object, Object>> readAllData(String tableName,Class rowType, String columnFamilyName,Map<Object, Class> qualifiersValueType): ",
					e);
		}
		return rowFamily;
	}

}
