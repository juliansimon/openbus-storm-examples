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
import java.util.HashSet;
import java.util.List;
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

import com.esotericsoftware.minlog.Log;
import com.produban.openbus.processor.properties.Conf;
import com.produban.openbus.processor.register.OpenTSDBRecoder;
import com.produban.openbus.processor.register.RemoteRecoder;
import com.produban.openbus.processor.util.FormatUtil;

/**
 * Function Storm/Trident for request and datetime hour    
 */
public class LogRealtimeIndicator extends BaseFunction {

	private static Logger LOG = LoggerFactory.getLogger(LogRealtimeIndicator.class);	
	private static final long serialVersionUID = -6300713191424691973L;
    	
	private long countRequest;
	private long start_time;
    private RemoteRecoder remoteRecoder = null;
	private HashSet<String> users = null;
	private long sumResponseTime;
	private long sumError404;
    
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		users = new HashSet<String>();
		
		remoteRecoder = new OpenTSDBRecoder();
		try {
			remoteRecoder.init(Conf.HOST_OPENTSDB, Conf.PORT_OPENTSDB);
		} catch (UnknownHostException e) {
			LOG.error("No host remote " + Conf.HOST_OPENTSDB + " " + Conf.PORT_OPENTSDB + " " + e);
		} catch (IOException e) {
			LOG.error("No connection host remote " + e);
		}
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {
    	JSONObject json = null;
        try {
            json = new JSONObject(tuple.getString(0));                                                  
            countRequest++;
            users.add(json.getString("user"));                        
            sumResponseTime += Integer.parseInt(json.getString("responseTime"));
            
            if (("404").equals(json.getString("status")))   sumError404++;
            
            if (System.currentTimeMillis() - start_time > Conf.TIME_PERIOD_MAX_RESPONSETIME) {            	
            	long timepPoint = System.currentTimeMillis() / 1000L;
            	String point = "put apache.request " + timepPoint + " " + Long.toString(countRequest) + " request=count\n";
            	//remoteRecoder.send(point);
            	LOG.info("Point count request " + point);
            	point = "put apache.request " + timepPoint + " " + users.size() + " users=count\n";
            	//remoteRecoder.send(point);
            	LOG.info("Point count users " + point);
            	point = "put apache.response " + timepPoint + " " + sumResponseTime/countRequest + " request=html\n";
            	//remoteRecoder.send(point);
            	LOG.info("Point responseTime average " + point);
            	point = "put apache.response " + timepPoint + " " + sumError404 + " error=404\n";
            	//remoteRecoder.send(point);
            	LOG.info("Point error 404 " + point);            	
            	//remoteRecoder.send(point);
                       	
            	// Reset
            	start_time = System.currentTimeMillis();
            	countRequest = 0;
            	users.clear();
            	sumError404 = 0;
            }
                        
            collector.emit(new Values(json.getString("request"), json.getString("user"), json.getString("responseTime"), json.getString("status")));               
        } catch (Exception e) {
        	LOG.error("Real-time indicators logs. Json: " + json, e);
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
