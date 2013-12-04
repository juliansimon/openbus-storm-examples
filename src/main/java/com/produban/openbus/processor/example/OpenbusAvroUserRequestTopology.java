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

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.function.AvroDecoderLogs;
import com.produban.openbus.processor.spout.OpenbusBrokerSpout;

public class OpenbusAvroUserRequestTopology {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusAvroUserRequestTopology.class);
		     		
	public static StormTopology buildTopology(LocalDRPC drpc) {		
		OpenbusBrokerSpout openbusBrokerSpout = new OpenbusBrokerSpout("_2_2_avro");
		
		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology.newStream("spout1", openbusBrokerSpout.getPartitionedTridentSpout())
	        		.each(new Fields("bytes"), new AvroDecoderLogs(), new Fields("user", "request"))
	        		//.each(new Fields("user", "request"), new LogFilter())
	        		.groupBy(new Fields("user", "request"))  // Particionar (repartir) por el campo name 
	        		.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
		
//		Stream documentStream = wordCounts
//        		.newValuesStream()
//        		.each(new Fields("user", "request", "count"), new LogFilter())  // Show tuple in console
//        		;
				
        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("user"))
                //.each(new Fields("user"), new LogFilter())
                .groupBy(new Fields("user"))
                .stateQuery(wordCounts, new Fields("user"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(1);		
		
		if (args.length == 0) {		
			LOG.debug("Storm mode local");
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("AvroDecoder", conf, buildTopology(drpc));		
		    
		    for(int i=0; i<2; i++) {
		    	LOG.info("Vale:  " + drpc.execute("words", "user815"));
		    }
		    Thread.sleep(1000);
		    
		     //cluster.shutdown();
		} else {
			LOG.debug("Storm mode cluster");
			conf.setNumWorkers(6);
			//StormSubmitter.submitTopology();
			//TODO: Create the twitter spout and pass it in here...
			//StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
	}
	
	//{"host": "85.155.188.197", "log": "-", "user": "user818", "datetime": "[17\/Sep\/2012:19:01:24+0200]", "request": "\"GET_\/Estatico\/Globales\/V114\/Bhtcs\/Internet\/AT\/\"", "status": "200", "size": "3117", "referer": "\"-\"", "userAgent": "\"Chrome\/21.0.1180.89\"", "session": "\"0000z2ur1hruUUG-MhpsITK9JY_:16vnisqka\"", "responseTime": "1020"}
}