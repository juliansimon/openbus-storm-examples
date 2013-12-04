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

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.produban.openbus.processor.function.ParseJSON;
import com.produban.openbus.processor.util.LogFilter;

public class OpenbusJSONStateTestTopology {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusJSONStateTestTopology.class);
		    
	public static StormTopology buildTopology() {	
				
		TridentTopology topology = new TridentTopology();
		 
		
// 		OpenbusBrokerSpout openbusBrokerSpout = new OpenbusBrokerSpout("jsonTopic1"); 	
// 		
//		TridentState wordCounts = topology.newStream("spout1", openbusBrokerSpout.getPartitionedTridentSpout())		
//        		.each(new Fields("bytes"), new ParseJSON(), new Fields("name","type"))
//        		.each(new Fields("name","type"), new LogFilter())
//        		.groupBy(new Fields("name"))
//        		.persistentAggregate(new MemoryMapState.Factory(), new CountConsole(), new Fields("count"));
		
		// TEST
		topology.newStream("spout", getSpoutTest())
				.each(new Fields("bytes"), new ParseJSON(), new Fields("name","type")) // Convertir array bytes en json 
        		.groupBy(new Fields("name"))  // Particionar (repartir) por el campo name 
        		.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")) // Se persiste el name y el resultado de la count
        		.newValuesStream()
        		.each(new Fields("name", "count"), new LogFilter())  // Mostrar la tupla en la consola
        		;
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(1);		
		
		
		if (args.length == 0) {		
			LOG.debug("Storm mode local");
			LocalCluster cluster = new LocalCluster();
			//LocalDRPC drpc = new LocalDRPC();
		    cluster.submitTopology("AvroDecoder", conf, buildTopology());	
		    //System.out.println("Nº de ocurrencias de Es: " + drpc.execute("words", "testFetchJSON1"));
		    
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
	
	private static IBatchSpout getSpoutTest() {
		FixedBatchSpout spout = null;
		
		try {
			spout = new FixedBatchSpout(new Fields("bytes"), 2,
		            new Values("{name:testFetchJSON2,type:2}".getBytes("UTF-8")),
		            new Values("{name:testFetchJSON1,type:1}".getBytes("UTF-8")),
		            new Values("{name:testFetchJSON1,type:3}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON1,type:4}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON6,type:6}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON1,type:4}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON1,type:5}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON1,type:8}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON9,type:9}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON10,type:10}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON11,type:11}".getBytes("UTF-8")),
//		            new Values("{name:testFetchJSON10,type:12}".getBytes("UTF-8")),
		            new Values("{name:testFetchJSON7,type:7}".getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) { }
		
		//spout.setCycle(true);
		
		return spout;
	}
}