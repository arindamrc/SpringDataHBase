package com.infy.hbase.schema.definition;

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;

public class ColumnFamilyDefinition {
	/** The divider between the column family name and a label. */
	public static final String DIV_COLUMN_LABEL = ":";
	/** Default values for HBase. */
	private static final int DEF_MAX_VERSIONS = HColumnDescriptor.DEFAULT_VERSIONS;
	/** Default values for HBase. */
	private static final String DEF_COMPRESSION_TYPE = HColumnDescriptor.DEFAULT_COMPRESSION;
	/** Default values for HBase. */
	private static final boolean DEF_IN_MEMORY = HColumnDescriptor.DEFAULT_IN_MEMORY;
	/** Default values for HBase. */
	private static final boolean DEF_BLOCKCACHE_ENABLED = HColumnDescriptor.DEFAULT_BLOCKCACHE;
	/** Default values for HBase. */
	private static final int DEF_TIME_TO_LIVE = HColumnDescriptor.DEFAULT_TTL;
	/** Default values for HBase. */
	private static final String DEF_BLOOM_FILTER = HColumnDescriptor.DEFAULT_BLOOMFILTER;

	private static final boolean DEF_MAP_TO_ENTITY = false;
	
	public ColumnFamilyDefinition() {
	}

	private String name;
	private String tableName;
	private String description;
	private int maxVersions = DEF_MAX_VERSIONS;
	private boolean inMemory = DEF_IN_MEMORY;
	private boolean blockCacheEnabled = DEF_BLOCKCACHE_ENABLED;
	private int timeToLive = DEF_TIME_TO_LIVE;
	private String compressionType = DEF_COMPRESSION_TYPE;
	private String bloomFilter = DEF_BLOOM_FILTER;
	private boolean mapToEntity = DEF_MAP_TO_ENTITY;
	private Collection<QualifierDefinition> qualifiers;
	private String entityName;


	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public boolean isMapToEntity() {
		return mapToEntity;
	}

	public void setMapToEntity(boolean mapToEntity) {
		this.mapToEntity = mapToEntity;
	}

	
	public Collection<QualifierDefinition> getQualifiers() {
		return qualifiers;
	}

	public void setQualifiers(Collection<QualifierDefinition> qualifiers) {
		this.qualifiers = qualifiers;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getMaxVersions() {
		return maxVersions;
	}

	public void setMaxVersions(int maxVersions) {
		this.maxVersions = maxVersions;
	}

	public boolean isInMemory() {
		return inMemory;
	}

	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}

	public boolean isBlockCacheEnabled() {
		return blockCacheEnabled;
	}

	public void setBlockCacheEnabled(boolean blockCacheEnabled) {
		this.blockCacheEnabled = blockCacheEnabled;
	}

	public int getTimeToLive() {
		return timeToLive;
	}

	public void setTimeToLive(int timeToLive) {
		this.timeToLive = timeToLive;
	}

	public String getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(String compressionType) {
		this.compressionType = compressionType;
	}

	public String getBloomFilter() {
		return bloomFilter;
	}

	public void setBloomFilter(String bloomFilter) {
		this.bloomFilter = bloomFilter;
	}

	public static class Builder {
		// Required parameters
		private String name;

		// additional parameters
		private String tableName;
		private String description;
		private int maxVersions = DEF_MAX_VERSIONS;
		private boolean inMemory = DEF_IN_MEMORY;
		private boolean blockCacheEnabled = DEF_BLOCKCACHE_ENABLED;
		private int timeToLive = DEF_TIME_TO_LIVE;
		private String compressionType = DEF_COMPRESSION_TYPE;
		private String bloomFilter = DEF_BLOOM_FILTER;
		private Collection<QualifierDefinition> qualifiers;
		private boolean mapToEntity = DEF_MAP_TO_ENTITY;

		public Builder(String name) {
			this.name = name;
		}

		public Builder tableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder description(String description) {
			this.description = description;
			return this;
		}

		public Builder maxVersions(int maxVersions) {
			this.maxVersions = maxVersions;
			return this;
		}

		public Builder inMemory(boolean inMemory) {
			this.inMemory = inMemory;
			return this;
		}
		
		public Builder mapToEntity(boolean mapToEntity) {
			this.mapToEntity = mapToEntity;
			return this;
		}

		public Builder blockCacheEnabled(boolean blockCacheEnabled) {
			this.blockCacheEnabled = blockCacheEnabled;
			return this;
		}

		public Builder timeToLive(int timeToLive) {
			this.timeToLive = timeToLive;
			return this;
		}

		public Builder compressionType(String compressionType) {
			this.compressionType = compressionType;
			return this;
		}

		public Builder bloomFilter(String bloomFilter) {
			this.bloomFilter = bloomFilter;
			return this;
		}

		public Builder qualifiers(Collection<QualifierDefinition> qualifiers) {
			this.qualifiers = qualifiers;
			return this;
		}

		public ColumnFamilyDefinition build() {
			return new ColumnFamilyDefinition(this);
		}

	}

	private ColumnFamilyDefinition(Builder builder) {
		name = builder.name;
		blockCacheEnabled = builder.blockCacheEnabled;
		bloomFilter = builder.bloomFilter;
		compressionType = builder.compressionType;
		description = builder.description;
		inMemory = builder.inMemory;
		maxVersions = builder.maxVersions;
		tableName = builder.tableName;
		timeToLive = builder.timeToLive;
		qualifiers = builder.qualifiers;
		mapToEntity=builder.mapToEntity;
	}

	/*
	 * public Map<Object, Object> getKeyValuePairs() { return keyValuePairs; }
	 * 
	 * public void setKeyValuePairs(Map<Object, Object> keyValuePairs) {
	 * this.keyValuePairs = keyValuePairs; }
	 * 
	 * public Object getValue(Object key) { return keyValuePairs.get(key); }
	 * 
	 * public void setValue(Object key,Object value) { keyValuePairs.put(key,
	 * value); }
	 */
}
