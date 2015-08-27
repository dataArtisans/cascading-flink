package com.dataArtisans.flinkCascading.runtime.stats;

public class EnumStringConverter {

	public static String enumToGroup(Enum e) {
		return e.getDeclaringClass().getName();
	}

	public static String enumToKey(Enum e) {
		return e.name();
	}

	public static String mergeGroupCounter(String group, String counter) {
		return group + "->" + counter;
	}

	public static boolean accInGroup(String group, String accKey) {
		return accKey.startsWith(group + "->");
	}

	public static boolean accMatchesGroupCounter(String accKey, String group, String counter) {
		return accKey.equals(group + "->" + counter);
	}

	public static String groupCounterToGroup(String groupKey) {
		return groupKey.split("->")[0];
	}

}
