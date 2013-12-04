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
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.function.Avro2HDFS;
import com.produban.openbus.processor.spout.OpenbusBrokerSpout;

public class OpenbusAvroHDFSTopology {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusAvroHDFSTopology.class);
		     		
	public static StormTopology buildTopology() {		
		OpenbusBrokerSpout openbusBrokerSpout = new OpenbusBrokerSpout();
		
		TridentTopology topology = new TridentTopology();
		Stream documentStream = topology.newStream("spout1", openbusBrokerSpout.getPartitionedTridentSpout())
        		.each(new Fields("bytes"), new Avro2HDFS(), new Fields("postBytes"));
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(1);		
		
		if (args.length == 0) {		
			LOG.debug("Storm mode local");
			LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("StoreHDFS", conf, buildTopology());						 		 		    
		    Thread.sleep(1000);
		     //cluster.shutdown();
		} else {
			LOG.debug("Storm mode cluster");
			conf.setNumWorkers(6);
			//StormSubmitter.submitTopology();
			//StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
	}
}