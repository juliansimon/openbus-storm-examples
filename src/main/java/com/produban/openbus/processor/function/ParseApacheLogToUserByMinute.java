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

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.produban.openbus.processor.util.FormatUtil;

/**
 * Function Storm/Trident for request and datetime hour    
 */
public class ParseApacheLogToUserByMinute extends BaseFunction {

	private static Logger LOG = LoggerFactory.getLogger(ParseApacheLogToUserByMinute.class);    
	private static final long serialVersionUID = 1L;
    
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {
    	JSONObject json = null;
        try {
            json = new JSONObject(tuple.getString(0));           
            String datetime = FormatUtil.getDateInFormatMinute(json.getString("datetime"));            
            collector.emit(new Values(json.getString("user"), datetime));               
        } catch (Exception e) { 
        	LOG.error("Parser JSON exception. Json: " + json, e);
		} 
    }
}
