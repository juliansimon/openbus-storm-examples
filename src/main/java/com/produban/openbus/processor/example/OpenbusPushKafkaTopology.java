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


import java.io.IOException;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.function.PushKafka;
import com.produban.openbus.processor.spout.OpenbusBrokerSpout;

public class OpenbusPushKafkaTopology {

	public static TridentTopology makeTopology() throws IOException {
		TridentTopology topology = new TridentTopology();

		OpenbusBrokerSpout openbusBrokerSpout = new OpenbusBrokerSpout("test"); 	
		
		Stream documentStream = topology.newStream("kafka", openbusBrokerSpout.getPartitionedTridentSpout())
        		.each(new Fields("bytes"), new PushKafka(), new Fields("text"));

		return topology;
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, makeTopology().build());
		} else {
			conf.setMaxTaskParallelism(2);					
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test-kafka-push", conf, makeTopology().build());
			Thread.sleep(10000);

			//cluster.shutdown();
		}
	}
}