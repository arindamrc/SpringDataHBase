package com.infy.hbasetest;

public class AhamTablesClasses {
	public static class ReplenishmentDetails {
		public static class ReplenishmentConfiguration {
			public static class ItemType {
				public static String value() {
					return "ITEM_TYPE";
				}
			}
			public static class ParentKey {
				public static String value() {
					return "PARENT_KEY";
				}
			}
			
			public static String value() {
				return "REPLENISHMENT_CONFIGURATION";
			}
		}

		public static String value() {
			return "REPLENISHMENT_DETAILS";
		}
	}
}
