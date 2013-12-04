/*
* Copyright 2013 Produban
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.produban.openbus.processor.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatting useful for date
 *
 */
public class FormatUtil {
	private static Logger LOG = LoggerFactory.getLogger(FormatUtil.class);
			
	public  static String DATE_FORMAT_LOG = "[dd/MMM/yyyy:HH:mm:ss+0200]";
	public  static String DATE_FORMAT_SECOND = "yyyyMMddHHmmss";
	public  static String DATE_FORMAT_MINUTE = "yyyyMMddHHmm";
	public  static String DATE_FORMAT_HOUR = "yyyyMMddHH";
	public  static String DATE_FORMAT_DAY = "yyyyMMdd";
	public  static String DATE_FORMAT_MONTH = "yyyyMM";	
	public static String DATE_FORMAT_WEBSERVER = "EEE MMM dd HH:mm:ss zzzz yyyy";
	public  static String DATE_FORMAT_YEARWEEK = "yyyyw";	
	
	public static String getDateFormat(String dateLogs, String oldFormatterStr, String newFormatterStr, Locale locale) {	
        DateFormat oldFormatter = new SimpleDateFormat(oldFormatterStr, locale);
		DateFormat newFormatter = new SimpleDateFormat(newFormatterStr, locale);
        Date oldDate = null;
        		
		try {
			oldDate = (Date)oldFormatter.parse(dateLogs);
		} catch (ParseException pe) {
			LOG.error("Error in parse. OldFormatter: " + oldFormatterStr + " newFormatter " + newFormatterStr + " " + pe);			 
		}
	    	    
	    return newFormatter.format(oldDate);
	}
	
	public static String getDateFormat(String dateLogs, String oldFormatterStr, String newFormatterStr) {
		return getDateFormat(dateLogs, oldFormatterStr, newFormatterStr, Locale.ENGLISH);
	}
	
	public static long getDateInFormat10Second(String dateLogs, Locale locale) {
        DateFormat oldFormatter = new SimpleDateFormat(DATE_FORMAT_WEBSERVER, locale); 
        Date oldDate = null;
		
		try {
			oldDate = (Date)oldFormatter.parse(dateLogs);
		} catch (ParseException pe) {
			LOG.error("Error in parse. OldFormatter: " + oldFormatter + ": " + pe);			 
		}
		return (oldDate.getTime() - (oldDate.getTime()%10000))/1000;		
	}
	
	public static long getDateInFormat10Second(String dateLogs) {
		return getDateInFormat10Second(dateLogs, Locale.ENGLISH);
	}	
	
	public static String getDateInFormatSecond(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_SECOND);
	}	
	
	public static String getDateInFormatMinute(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_MINUTE);
	}
	
	public static String getDateInFormatHour(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_HOUR);
	}
	
	public static String getDateInFormatDay(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_DAY);
	}	
		
	public static long getDateInFormatTimeStamp(String dateLogs) {
		return getDateInFormatTimeStamp(dateLogs, Locale.ENGLISH);
	}
	
	public static long getDateInFormatTimeStamp(String dateLogs, Locale locale) {
		DateFormat formatter = new SimpleDateFormat(DATE_FORMAT_WEBSERVER, locale); 		
		Date date = null;
		long unixtime;
			
		try {
			date = (Date)formatter.parse(dateLogs);
		}
		catch (ParseException pe) {
			LOG.error("Error in parse: " + formatter + ": " + pe);	
		}
		
		unixtime = date.getTime() / 1000L;
		
		return unixtime;
	}
	
	public static long getDateInFormatSecond(String dateLogs, int seconds) {
		return getDateInFormatSecond(dateLogs, seconds, Locale.ENGLISH);
	}
	
	/**
	 * Transform date in format by seconds
	 * 
	 * @param dateLogs date
	 * @param seconds seconds 
	 * @param locale 
	 * @return
	 */
	public static long getDateInFormatSecond(String dateLogs, int seconds, Locale locale) {
        DateFormat oldFormatter = new SimpleDateFormat(DATE_FORMAT_WEBSERVER, locale); 
        Date oldDate = null;
		
		try {
			oldDate = (Date)oldFormatter.parse(dateLogs);
		} catch (ParseException pe) {
			LOG.error("Error in parse. OldFormatter: " + oldFormatter + ": " + pe);			 
		}
		return (oldDate.getTime() - (oldDate.getTime()%(1000*seconds)))/1000;		
	}
	
	/***
	 * 
	 * @param request
	 * @return
	 */
	public static String getRequestFormat(String request) {		
		request = request.replaceAll("GET_/", "");		
		return request.replaceAll("\"", "");
	}
}