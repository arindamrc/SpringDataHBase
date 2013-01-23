package com.infy.hbase.dao.support;

import static com.infy.hbase.utilities.ByteArrayConversionUtilities.getByteArray;
import static com.infy.hbase.utilities.ByteArrayConversionUtilities.getObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;


public class SpringDataDaoSupport implements HBaseDaoSupport {

	private static final Logger log = LoggerFactory
			.getLogger(SpringDataDaoSupport.class);

	private HbaseTemplate hBaseTemplate;

	public void sethBaseTemplate(HbaseTemplate hBaseTemplate) {
		this.hBaseTemplate = hBaseTemplate;
	}

	public void addOrUpdateData(final String tableName, final Object rowKey,
			final String columnFamilyName,
			final Map<Object, Object> qualifierValueMap) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Put p = new Put(getByteArray(rowKey));
					for (Map.Entry<Object, Object> entry : qualifierValueMap
							.entrySet()) {
						p.add(getByteArray(columnFamilyName),
								getByteArray(entry.getKey()),
								getByteArray(entry.getValue()));
					}
					table.put(p);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public void addOrUpdateData(final String tableName, final Object rowKey,
			final String columnFamilyName,
			final Map<Object, Object> qualifierValueMap, final long timestamp) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Put p = new Put(getByteArray(rowKey));
					for (Map.Entry<Object, Object> entry : qualifierValueMap
							.entrySet()) {
						p.add(getByteArray(columnFamilyName),
								getByteArray(entry.getKey()), timestamp,
								getByteArray(entry.getValue()));
					}
					table.put(p);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public Map<Object, Object> readData(final String tableName,
			final Object rowKey, final String columnFamilyName,
			final Map<Object, Class> qualifiersValueType) {
		
		Map<Object, Object> qualifierValueMap = hBaseTemplate.execute(
				tableName, new TableCallback<Map<Object, Object>>() {
					public Map<Object, Object> doInTable(HTable table)
							throws Throwable {
						Map<Object, Object> tempQualifierValueMap = new HashMap<Object, Object>();
						try {
							Get get = new Get(getByteArray(rowKey));
							Result result = table.get(get);
							byte[] valueByte;
							for (Map.Entry<Object, Class> entry : qualifiersValueType
									.entrySet()) {
								valueByte = result.getValue(
										Bytes.toBytes(columnFamilyName),
										getByteArray(entry.getKey()));
								if (valueByte != null) {
									tempQualifierValueMap.put(
											entry.getKey(),
											getObject(valueByte,
													entry.getValue()));
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							throw hBaseTemplate.convertHbaseAccessException(e);
						} finally {
							if (table != null) {
								table.close();
							}
						}
						return tempQualifierValueMap;
					}
				});

		return qualifierValueMap;
	}

	public Map<Object, Object> readData(final String tableName,
			final Object rowKey, final String columnFamilyName,
			final Map<Object, Class> qualifiersValueType, final long timeStamp) {

		Map<Object, Object> qualifierValueMap = hBaseTemplate.execute(
				tableName, new TableCallback<Map<Object, Object>>() {
					public Map<Object, Object> doInTable(HTable table)
							throws Throwable {
						Map<Object, Object> tempQualifierValueMap = new HashMap<Object, Object>();
						try {
							Get get = new Get(getByteArray(rowKey));
							get.setTimeStamp(timeStamp);
							Result result = table.get(get);
							byte[] valueByte;
							for (Map.Entry<Object, Class> entry : qualifiersValueType
									.entrySet()) {
								valueByte = result.getValue(
										Bytes.toBytes(columnFamilyName),
										getByteArray(entry.getKey()));
								if (valueByte != null) {
									tempQualifierValueMap.put(
											entry.getKey(),
											getObject(valueByte,
													entry.getValue()));
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							throw hBaseTemplate.convertHbaseAccessException(e);
						} finally {
							if (table != null) {
								table.close();
							}
						}
						return tempQualifierValueMap;
					}
				});

		return qualifierValueMap;
	}

	public void deleteData(final String tableName, final Object rowKey,
			final String columnFamilyName, final Set<Object> qualifiers) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Delete delete = new Delete(getByteArray(rowKey));
					for (Object qualifier : qualifiers) {
						delete.deleteColumn(Bytes.toBytes(columnFamilyName),
								getByteArray(qualifier));
					}
					table.delete(delete);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public void deleteData(final String tableName, final Object rowKey,
			final String columnFamilyName,
			final Map<Object, Long> qualifierTimeStamp) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Delete delete = new Delete(getByteArray(rowKey));
					for (Map.Entry<Object, Long> entry : qualifierTimeStamp
							.entrySet()) {
						delete.deleteColumn(Bytes.toBytes(columnFamilyName),
								getByteArray(entry.getKey()), entry.getValue());
					}
					table.delete(delete);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public void deleteData(final String tableName, final Object rowKey) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Delete delete = new Delete(getByteArray(rowKey));
					table.delete(delete);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public void deleteData(final String tableName, final Object rowKey,
			final Long timeStamp) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Delete delete = new Delete(getByteArray(rowKey));
					delete.setTimestamp(timeStamp);
					table.delete(delete);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public void deleteData(final String tableName, final Object rowKey,
			final String columnFamilyName) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Delete delete = new Delete(getByteArray(rowKey));
					delete.deleteFamily(Bytes.toBytes(columnFamilyName));
					table.delete(delete);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public void deleteData(final String tableName, final Object rowKey,
			final String columnFamilyName, final Long timeStamp) {
		hBaseTemplate.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				try {
					Delete delete = new Delete(getByteArray(rowKey));
					delete.deleteFamily(Bytes.toBytes(columnFamilyName),
							timeStamp);
					table.delete(delete);
				} catch (Exception e) {
					e.printStackTrace();
					throw hBaseTemplate.convertHbaseAccessException(e);
				} finally {
					if (table != null) {
						table.close();
					}
				}
				return null;
			}
		});
	}

	public Map<Object, Map<Object, Object>> readAllData(final String tableName,
			final Class rowType, final String columnFamilyName,
			final Map<Object, Class> qualifiersValueType) {
		final Map<Object, Map<Object, Object>> rowFamily = new HashMap<Object, Map<Object, Object>>();
		hBaseTemplate.find(tableName, columnFamilyName,
				new RowMapper<Map<Object, Map<Object, Object>>>() {
					public Map<Object, Map<Object, Object>> mapRow(
							Result result, int rowNum) throws Exception {
						Map<Object, Object> qualifierValueMap = null;
						try {
							qualifierValueMap = new HashMap<Object, Object>();
							for (Map.Entry<Object, Class> entry : qualifiersValueType
									.entrySet()) {
								Object value = null;
								try {
									value = getObject(result.getValue(
											Bytes.toBytes(columnFamilyName),
											getByteArray(entry.getKey())),
											entry.getValue());
								} catch (NullPointerException e) {
									log.error(
											"No such key in family: "
													+ entry.getKey() + " "
													+ columnFamilyName, e);
								}
								catch (NumberFormatException e) {
									log.error(
											"value type mismatch for key: "
													+ entry.getKey() + " "
													+ entry.getValue(), e);
								}
								if (value != null) {
									qualifierValueMap.put(entry.getKey(), value);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							throw hBaseTemplate.convertHbaseAccessException(e);
						}
						rowFamily.put(getObject(result.getRow(), rowType),
								qualifierValueMap);
						return null;
					}
				});

		return rowFamily;
	}
}
