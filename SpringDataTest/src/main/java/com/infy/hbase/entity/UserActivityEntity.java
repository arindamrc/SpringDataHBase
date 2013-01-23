package com.infy.hbase.entity;

import com.infy.hbase.entity.annotations.HBaseColumnFamily;
import com.infy.hbase.entity.annotations.HBaseQualifierName;
import com.infy.hbase.entity.annotations.HBaseTable;

@HBaseTable(name="USER_RECENT_ACTIVITY")
@HBaseColumnFamily(name="PURCHASE")
public class UserActivityEntity extends AbstractHBaseEntity {
	
	@HBaseQualifierName(value="ACTIVITYTYPE")
	private String activityType;
	
	@HBaseQualifierName(value="DESCRIPTION")
	private String description;
	
	@HBaseQualifierName(value="CREATIONDATE")
	private Long creationDate;
	
	@HBaseQualifierName(value="ACTIVITYVALUE")
	private String activityValue;
	
	@HBaseQualifierName(value="SITE")
	private Long site;
	
	@HBaseQualifierName(value="LINK")
	private String link;

	public String getActivityType() {
		return activityType;
	}

	public void setActivityType(String activityType) {
		this.activityType = activityType;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Long creationDate) {
		this.creationDate = creationDate;
	}

	public String getActivityValue() {
		return activityValue;
	}

	public void setActivityValue(String activityValue) {
		this.activityValue = activityValue;
	}

	public Long getSite() {
		return site;
	}

	public void setSite(Long site) {
		this.site = site;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	@Override
	public String toString() {
		return "UserActivityEntity [activityType=" + activityType
				+ ", description=" + description + ", creationDate="
				+ creationDate + ", activityValue=" + activityValue + ", site="
				+ site + ", link=" + link + ", rowKey=" + rowKey
				+ ", timeStamp=" + timeStamp + "]";
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((activityType == null) ? 0 : activityType.hashCode());
		result = prime * result
				+ ((activityValue == null) ? 0 : activityValue.hashCode());
		result = prime * result
				+ ((creationDate == null) ? 0 : creationDate.hashCode());
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((link == null) ? 0 : link.hashCode());
		result = prime * result + ((site == null) ? 0 : site.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (!(obj instanceof UserActivityEntity)) {
			return false;
		}
		UserActivityEntity other = (UserActivityEntity) obj;
		if (activityType == null) {
			if (other.activityType != null) {
				return false;
			}
		} else if (!activityType.equals(other.activityType)) {
			return false;
		}
		if (activityValue == null) {
			if (other.activityValue != null) {
				return false;
			}
		} else if (!activityValue.equals(other.activityValue)) {
			return false;
		}
		if (creationDate == null) {
			if (other.creationDate != null) {
				return false;
			}
		} else if (!creationDate.equals(other.creationDate)) {
			return false;
		}
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (link == null) {
			if (other.link != null) {
				return false;
			}
		} else if (!link.equals(other.link)) {
			return false;
		}
		if (site == null) {
			if (other.site != null) {
				return false;
			}
		} else if (!site.equals(other.site)) {
			return false;
		}
		return true;
	}
	
}
