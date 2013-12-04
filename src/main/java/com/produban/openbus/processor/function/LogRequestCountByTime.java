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


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;

import com.produban.openbus.processor.properties.Conf;
import com.produban.openbus.processor.register.OpenTSDBRecoder;
import com.produban.openbus.processor.register.RemoteRecoder;
import com.produban.openbus.processor.util.FormatUtil;

/**
 * Function Storm/Trident for request and datetime hour    
 */
public class LogRequestCountByTime extends BaseFunction {

	private static Logger LOG = LoggerFactory.getLogger(LogRequestCountByTime.class);    
	private static final long serialVersionUID = 1L;
	//private static final long TIME_MAX = 5000; // Milis seconds
	
	private long count;
	private long start_time;
    private RemoteRecoder remoteRecoder = null;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		
//		remoteRecoder = new OpenTSDBRecoder();
//		try {
//			remoteRecoder.init(Conf.HOST_OPENTSDB, Conf.PORT_OPENTSDB);
//		} catch (UnknownHostException e) {
//			LOG.error("No host remote " + Conf.HOST_OPENTSDB + " " + Conf.PORT_OPENTSDB + " " + e);
//			throw new RuntimeException(e);
//		} catch (IOException e) {
//			LOG.error("No connection host remote " + e);
//			throw new RuntimeException(e);
//		}
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {        
        try {
            JSONObject json = new JSONObject(tuple.getString(0));                                                  
            count++;

            // Window time
            if (System.currentTimeMillis() - start_time >  Conf.TIME_PERIOD_MAX_RESPONSETIME) {
            	// Send
            	String point = "put storm.wordcount " + System.currentTimeMillis() / 1000L + " " + Long.toString(count) + " request=count\n";
            	LOG.info("point: " + point);
            	//remoteRecoder.send(point);
                       	
            	// Reset
            	start_time = System.currentTimeMillis();
            	count = 0;
            }
                        
            collector.emit(new Values(json.getString("request")));               
        } catch (Exception e) {
        	LOG.error("Caught JSONException: " + e.getMessage()); 
        	throw new RuntimeException(e);
		} 
    }
    
    @Override
    public void cleanup() {
    	try {
    		if (remoteRecoder != null) remoteRecoder.close();
		} catch (IOException e) {
			LOG.error("No close connection " + e);
		}
    }      
}
