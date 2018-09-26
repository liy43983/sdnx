package cn.trustfar.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class DateTimeUtil {
	final public static String DATA_TIME_FORMAT_MILLI_SECOND_23 = "yyyy-MM-dd HH:mm:ss.SSS";
	final public static String DATA_TIME_FORMAT_SECOND_19 = "yyyy-MM-dd HH:mm:ss";
	final public static String DATA_TIME_FORMAT_MINUTE_16 = "yyyy-MM-dd HH:mm";
	final public static String DATA_TIME_FORMAT_HOUR_13 = "yyyy-MM-dd HH";
	final public static String DATA_TIME_FORMAT_DAY_10 = "yyyy-MM-dd";
	final public static String DATA_TIME_FORMAT_DAY_7 = "yyyy-MM";

	final public static String DATA_TIME_FORMAT_DAY_6 = "yyyyMM";
	final public static String DATA_TIME_FORMAT_DAY_8 = "yyyyMMdd";

	public static boolean whetherInThisInterval(DateTime text, DateTime start, DateTime end) {
		if (text.isAfter(start) && text.isBefore(end)) {
			return true;
		}
		return false;
	}

	public static DateTime parseDateTime(String text, String pattern) {
		if (text == null || text.length() < 5) {
			return null;
		}
		if (pattern != null && pattern.length() > 0) {
			return DateTimeFormat.forPattern(pattern).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_MILLI_SECOND_23.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_MILLI_SECOND_23).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_SECOND_19.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_SECOND_19).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_MINUTE_16.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_MINUTE_16).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_HOUR_13.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_HOUR_13).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_DAY_10.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_DAY_10).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_DAY_7.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_DAY_7).parseDateTime(text);
		}

		if (text.length() == DATA_TIME_FORMAT_DAY_8.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_DAY_8).parseDateTime(text);
		}
		if (text.length() == DATA_TIME_FORMAT_DAY_6.length()) {
			return DateTimeFormat.forPattern(DATA_TIME_FORMAT_DAY_6).parseDateTime(text);
		}
		return null;
	}

	/**
	 * 获取传入时间的当天开始时间,默认值为今天开始时间 
	 * @Title: getThedayStart 
	 * @Description: @param theDay  @return DateTime 
	 * @throws
	 */
	public static DateTime getThedayStart(DateTime theDay) {
		if (theDay == null) {
			theDay = new DateTime();
		}
		return theDay.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
	}
	
	/**
	 * 获取传入时间的当天结束时间,默认值为今天开始时间 
	 * @param theDay
	 * @return
	 */
	public static DateTime getThedayEnd(DateTime theDay) {
		if (theDay == null) {
			theDay = new DateTime();
		}
		return theDay.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999);
	}


	

}
