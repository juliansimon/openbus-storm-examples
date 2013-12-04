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

import com.produban.openbus.processor.util.FormatUtil;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * Convert Web Server logs messages Avro format Json format    
 */
public class WebServerLog2Json extends BaseFunction {
	
	private static Logger LOG = LoggerFactory.getLogger(WebServerLog2Json.class);
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {        
        try {        	        	
            JSONObject json = new JSONObject(tuple.getString(0));
            if (LOG.isDebugEnabled()) {
            	LOG.debug("Tuple: " + tuple.getString(0));
            	LOG.debug("Message: " + json.toString());
            }
            String datetime = FormatUtil.getDateInFormatMinute(json.getString("datetime"));
		    collector.emit(new Values(json.get("host"), json.get("log"), json.get("user"), datetime, 
		    		json.get("request"), json.get("status"), json.get("size"), json.get("referer"), 
		    		json.get("userAgent"), json.get("session"), json.get("responseTime"), json.toString())
		    );               
        } catch (Exception e) {
        	LOG.error("Caught JSONException: " + tuple.getString(0), e); 
		} 
    }
}