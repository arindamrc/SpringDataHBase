package com.infy.hbase.schema.conversion.hbase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.QualifierDefinition;
import com.infy.hbase.schema.definition.TableSchemaDefinition;

public class HBaseSchemaConversionUtility {
	public static TableSchemaDefinition convertDescriptorToSchema(
			final HTableDescriptor tableDescriptor) {
		TableSchemaDefinition tableSchemaDefinition = new TableSchemaDefinition();
		tableSchemaDefinition.setTableName(tableDescriptor.getNameAsString());
		Collection<ColumnFamilyDefinition> columnFamilyDefinitions = new ArrayList<ColumnFamilyDefinition>();
		Collection<HColumnDescriptor> hColumnDescriptors = tableDescriptor
				.getFamilies();
		for (HColumnDescriptor columnDescriptor : hColumnDescriptors) {
			ColumnFamilyDefinition columnFamilyDefinition = HBaseSchemaConversionUtility
					.getColumnDefinition(columnDescriptor);
			columnFamilyDefinition.setTableName(tableDescriptor
					.getNameAsString());
			columnFamilyDefinitions.add(columnFamilyDefinition);
		}
		tableSchemaDefinition.setColumnFamilies(columnFamilyDefinitions);
		return tableSchemaDefinition;
	}

	public static HTableDescriptor convertSchemaToDescriptor(
			final TableSchemaDefinition tableSchemaDefinition) {
		HTableDescriptor desc;
		desc = new HTableDescriptor(tableSchemaDefinition.getTableName());
		Collection<ColumnFamilyDefinition> cols = tableSchemaDefinition
				.getColumnFamilies();
		for (ColumnFamilyDefinition col : cols) {
			HColumnDescriptor cd = new HBaseSchemaConversionUtility()
					.getColumnDescriptor(col);
			desc.addFamily(cd);
		}
		return desc;
	}

	private HColumnDescriptor getColumnDescriptor(
			final ColumnFamilyDefinition col) {
		HColumnDescriptor cd = new HColumnDescriptor(Bytes.toBytes(col
				.getName()), col.getMaxVersions(), col.getCompressionType(),
				col.isInMemory(), col.isBlockCacheEnabled(),
				col.getTimeToLive(), col.getBloomFilter());

		return cd;
	}

	private static ColumnFamilyDefinition getColumnDefinition(
			final HColumnDescriptor cd) {
		ColumnFamilyDefinition columnFamilyDefinition = new ColumnFamilyDefinition.Builder(
				cd.getNameAsString())
				.blockCacheEnabled(cd.isBlockCacheEnabled())
				.bloomFilter(cd.BLOOMFILTER).compressionType(cd.COMPRESSION)
				.inMemory(cd.isInMemory()).maxVersions(cd.getMaxVersions())
				.timeToLive(cd.getTimeToLive()).mapToEntity(false).build();
		/*Map<ImmutableBytesWritable, ImmutableBytesWritable> keyValueMap = cd
				.getValues();
		Collection<ImmutableBytesWritable> qualifiers = keyValueMap.keySet();
		Collection<QualifierDefinition> qualifierDefinitions = new ArrayList<QualifierDefinition>();
		for (ImmutableBytesWritable qualifier : qualifiers) {
			qualifierDefinitions.add(new QualifierDefinition(Bytes
					.toString(qualifier.get())));
		}
		columnFamilyDefinition.setQualifiers(qualifierDefinitions);*/
		return columnFamilyDefinition;
	}
}
