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


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.produban.openbus.processor.hdfs.HDFSStore;

/**
 * Function Storm/Trident for parse Json and persistence in HDFS   
 */
public class ParseJSON extends BaseFunction {

	private static Logger LOG = LoggerFactory.getLogger(ParseJSON.class);    
	private static final long serialVersionUID = 1L;
    
	private HDFSStore hDFSStore;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// Persitence in HDFS
		hDFSStore = new HDFSStore();
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {        
        try {            	
            String decoded = new String(tuple.getBinary(0));
            JSONObject json = new JSONObject(decoded);

            // Date message
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();
            String dateNow = dateFormat.format(cal.getTime());
            json.accumulate("date", dateNow);
            LOG.info("##### name: " + json.get("name") + " type: " + json.get("type") + " date: " + json.getString("date"));

            // Persitence in HDFS    			
    		hDFSStore.writeFile2HDFS("json" + "_" + System.nanoTime(), json.toString());
            
            collector.emit(new Values(json.getString("name"), json.getString("type")));               
        } catch (Exception e) {
        	LOG.error("Caught JSONException: " + e.getMessage()); 
        	throw new RuntimeException(e);
		} 
    }
}
