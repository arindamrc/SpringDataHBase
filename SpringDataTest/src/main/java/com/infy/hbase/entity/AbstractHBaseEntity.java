package com.infy.hbase.entity;

public abstract class AbstractHBaseEntity {
	protected Object rowKey;
	protected long timeStamp;
	public Object getRowKey() {
		return rowKey;
	}
	public void setRowKey(Object rowKey) {
		this.rowKey = rowKey;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rowKey == null) ? 0 : rowKey.hashCode());
		result = prime * result + (int) (timeStamp ^ (timeStamp >>> 32));
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
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof AbstractHBaseEntity)) {
			return false;
		}
		AbstractHBaseEntity other = (AbstractHBaseEntity) obj;
		if (rowKey == null) {
			if (other.rowKey != null) {
				return false;
			}
		} else if (!rowKey.equals(other.rowKey)) {
			return false;
		}
		if (timeStamp != other.timeStamp) {
			return false;
		}
		return true;
	}
	
}
