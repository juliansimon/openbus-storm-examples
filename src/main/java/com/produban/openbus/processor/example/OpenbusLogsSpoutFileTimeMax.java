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

package com.produban.openbus.processor.example;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.function.LogMaxResponseTime;
import com.produban.openbus.processor.function.ParseJSONLogHour;
import com.produban.openbus.processor.function.ParseJSONLogUserRequest;
import com.produban.openbus.processor.spout.SimpleFileStringSpout;
import com.produban.openbus.processor.util.LogFilter;
import com.produban.openbus.processor.util.UserOperationsLastList;

public class OpenbusLogsSpoutFileTimeMax {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusLogsSpoutFileTimeMax.class);
		    
	public static StormTopology buildTopology() {	
				
		TridentTopology topology = new TridentTopology();
		 
		SimpleFileStringSpout spout = new SimpleFileStringSpout("data/webapplogs.json", "rawLogs");
		spout.setCycle(true);
		
		Stream termStream = topology.newStream("spout", spout)				
				.each(new Fields("rawLogs"), new LogMaxResponseTime(), new Fields("responseTime"));
		
	
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(1);		
		
		
		if (args.length == 0) {		
			LOG.debug("Storm mode local");
			LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("timeMax", conf, buildTopology());	
		    Thread.sleep(1000);
//		    for(int i=0; i<10; i++) {
//		    	LOG.info("########### DRPC RESULT: " + drpc.execute("dQuery", "index.html"));
//		    	 Thread.sleep(1000);
//		    }
		    
		   
		     //cluster.shutdown();
		} else {
			LOG.debug("Storm mode cluster");
			conf.setNumWorkers(6);
			//StormSubmitter.submitTopology();
			//StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
	}		
}