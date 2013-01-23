package com.infy.hbase.entity;

import com.infy.hbase.entity.annotations.HBaseColumnFamily;
import com.infy.hbase.entity.annotations.HBaseQualifierName;
import com.infy.hbase.entity.annotations.HBaseTable;


/**
 * @author arindam_r
 *
 */
@HBaseTable(name = "PRODUCT_INSIGHT")
@HBaseColumnFamily(name = "PRODUCT_INFO")
public class Product extends AbstractHBaseEntity {

	@HBaseQualifierName(value = "NAME")
	private String partName;

	@HBaseQualifierName(value = "IMAGE_URL")
	private String imgId;

	@HBaseQualifierName(value = "CATG_NAME")
	private String category;

	@HBaseQualifierName(value = "VERTICAL_NAME")
	private String vertical;

	@HBaseQualifierName(value = "SUBCATEG_NAME")
	private String subCategory;
	
	private String partNumber;

	private String brand;
	private String lastPurchasedDate;
	private Integer numberOfTimesViewed;
	private Integer viewsInLastYear;
	private String saveStory;
	
	// Float rating is to be used for reading the recommendation rating
	// Not a general property of the product, depends on the context of the
	// recommendation
	// Added during productPageRefinement
	private float rating;

	private long count;
	private long creationTime;
	private long originalCount;
	private String percentViewed;

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getSubCategory() {
		return subCategory;
	}

	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}

	public String getVertical() {
		return vertical;
	}

	public void setVertical(String vertical) {
		this.vertical = vertical;
	}

	public String getPercentViewed() {
		return percentViewed;
	}

	public void setPercentViewed(String percentViewed) {
		this.percentViewed = percentViewed;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public String getPartNumber() {
		return rowKey.toString();
	}

	public void setPartNumber(String partNumber) {
		this.partNumber = rowKey.toString();
	}

	public String getPartName() {
		return partName;
	}

	public void setPartName(String partName) {
		this.partName = partName;
	}

	public String getImgId() {
		return imgId;
	}

	public void setImgId(String imgId) {
		this.imgId = imgId;
	}

	public String getSaveStory() {
		return saveStory;
	}

	public void setSaveStory(String saveStory) {
		this.saveStory = saveStory;
	}

	public long getOriginalCount() {
		return originalCount;
	}

	public void setOriginalCount(long originalCount) {
		this.originalCount = originalCount;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getBrand() {
		return brand;
	}

	@Override
	public String toString() {
		return "Product [partNumber=" + getPartNumber() + ", partName=" + partName
				+ ", category=" + category + ", vertical=" + vertical
				+ ", subCategory=" + subCategory + "]\n";
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public float getRating() {
		return rating;
	}

	public void setRating(float rating) {
		this.rating = rating;
	}

	public String getLastPurchasedDate() {
		return lastPurchasedDate;
	}

	public void setLastPurchasedDate(String lastPurchasedDate) {
		this.lastPurchasedDate = lastPurchasedDate;
	}

	public Integer getNumberOfTimesViewed() {
		return numberOfTimesViewed;
	}

	public void setNumberOfTimesViewed(Integer numberOfTimesViewed) {
		this.numberOfTimesViewed = numberOfTimesViewed;
	}

	public Integer getViewsInLastYear() {
		return viewsInLastYear;
	}

	public void setViewsInLastYear(Integer viewsInLastYear) {
		this.viewsInLastYear = viewsInLastYear;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((category == null) ? 0 : category.hashCode());
		result = prime * result
				+ ((partName == null) ? 0 : partName.hashCode());
		result = prime * result
				+ ((partNumber == null) ? 0 : partNumber.hashCode());
		result = prime * result
				+ ((subCategory == null) ? 0 : subCategory.hashCode());
		result = prime * result
				+ ((vertical == null) ? 0 : vertical.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Product other = (Product) obj;
		if (category == null) {
			if (other.category != null)
				return false;
		} else if (!category.equals(other.category))
			return false;
		if (partName == null) {
			if (other.partName != null)
				return false;
		} else if (!partName.equals(other.partName))
			return false;
		if (partNumber == null) {
			if (other.partNumber != null)
				return false;
		} else if (!partNumber.equals(other.partNumber))
			return false;
		if (subCategory == null) {
			if (other.subCategory != null)
				return false;
		} else if (!subCategory.equals(other.subCategory))
			return false;
		if (vertical == null) {
			if (other.vertical != null)
				return false;
		} else if (!vertical.equals(other.vertical))
			return false;
		return true;
	}

}
