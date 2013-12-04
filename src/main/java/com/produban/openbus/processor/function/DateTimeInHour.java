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

package com.produban.openbus.processor.function;


import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.produban.openbus.processor.properties.OpenbusConstant;
import com.produban.openbus.processor.util.FormatUtil;

/**
 * Function Storm/Trident for convert datetime in format hour    
 */
public class DateTimeInHour extends BaseFunction {
	private static final long serialVersionUID = -2429166054982714914L;
	private static Logger LOG = LoggerFactory.getLogger(DateTimeInHour.class);    
    	
	@Override
	@SuppressWarnings("rawtypes") 
	public void prepare(Map conf, TridentOperationContext context) {
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {        
        try {
        	String datetimeInHour = FormatUtil.getDateFormat(tuple.getString(1), 
        			FormatUtil.DATE_FORMAT_WEBSERVER, FormatUtil.DATE_FORMAT_HOUR);
                        
            if (LOG.isDebugEnabled())  LOG.debug("Datetime in hour: " + datetimeInHour);
                        
            collector.emit(new Values(tuple.getString(0) + OpenbusConstant.SEPARATOR_FIELDS + datetimeInHour));            
        } catch (Exception e) {
        	LOG.error("Format timestamp hour: " + tuple.getString(0) + " " + tuple.getString(1) 
        			+ " " + tuple.getStringByField("timestamp"), e); 
		} 
    }
}