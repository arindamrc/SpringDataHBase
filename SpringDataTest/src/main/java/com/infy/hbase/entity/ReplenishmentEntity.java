package com.infy.hbase.entity;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.infy.hbase.entity.annotations.HBaseColumnFamily;
import com.infy.hbase.entity.annotations.HBaseQualifierName;
import com.infy.hbase.entity.annotations.HBaseTable;

@HBaseTable(name = "REPLENISHMENT_DETAILS")
@HBaseColumnFamily(name = "REPLENISHMENT_CONFIGURATION")
public class ReplenishmentEntity extends AbstractHBaseEntity {

	@HBaseQualifierName(value = "FREQUENCY_MANUAL")
	private Double manualFrequency;

	@HBaseQualifierName(value = "FREQUENCY_COMPUTED")
	private Double computedFrequency;

	@HBaseQualifierName(value = "STANDARD_DEVIATION_MANUAL")
	private Double manualDeviation;

	@HBaseQualifierName(value = "STANDARD_DEVIATION_COMPUTED")
	private Double computedDeviation;

	@HBaseQualifierName(value = "ITEM_TYPE")
	private String itemType;

	@HBaseQualifierName(value = "PARENT_KEY")
	private String parentKey;

	@HBaseQualifierName(value = "USER_PREFERENCE")
	private String userPreference;

	public Double getManualFrequency() {
		return manualFrequency;
	}

	public void setManualFrequency(Double manualFrequency) {
		this.manualFrequency = manualFrequency;
	}

	public Double getComputedFrequency() {
		return computedFrequency;
	}

	public void setComputedFrequency(Double computedFrequency) {
		this.computedFrequency = computedFrequency;
	}

	public Double getManualDeviation() {
		return manualDeviation;
	}

	public void setManualDeviation(Double manualDeviation) {
		this.manualDeviation = manualDeviation;
	}

	public Double getComputedDeviation() {
		return computedDeviation;
	}

	public void setComputedDeviation(Double computedDeviation) {
		this.computedDeviation = computedDeviation;
	}

	public String getItemType() {
		return itemType;
	}

	public void setItemType(String itemType) {
		this.itemType = itemType;
	}

	public String getParentKey() {
		return parentKey;
	}

	public void setParentKey(String parentKey) {
		this.parentKey = parentKey;
	}

	public String getUserPreference() {
		return userPreference;
	}

	public void setUserPreference(String userPreference) {
		this.userPreference = userPreference;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || this.getClass() != obj.getClass()) {
			return false;
		}

		ReplenishmentEntity entity = (ReplenishmentEntity) obj;

		return new EqualsBuilder().append(this.rowKey, entity.rowKey)
				.append(entity.getParentKey(), entity.getParentKey())
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31).append(this.rowKey)
				.append(this.computedDeviation).append(this.computedFrequency)
				.append(this.itemType).append(this.manualDeviation)
				.append(this.manualFrequency).append(this.parentKey)
				.append(this.userPreference).toHashCode();
	}
}
