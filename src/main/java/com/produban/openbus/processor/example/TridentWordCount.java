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
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;


public class TridentWordCount {    
    
	private static Logger LOG = LoggerFactory.getLogger(TridentWordCount.class);
	
    public static StormTopology buildTopology(LocalDRPC drpc) {
    	
    	// Spout for the test 
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("title", "text"), 3,
                new Values("Don Quijote de la Mancha", "Es una novela escrita por Miguel de Cervantes Saavedra. Publicada su primera parte con el titulo de El ingenioso hidalgo don Quijote de la Mancha a comienzos de 1605"),
                new Values("El nombre de la rosa", "Es una novela de misterio e historica de Umberto Eco publicada en 1980"),
                new Values("Lazarillo de Tormes", "Es una novela espa�ola an�nima, escrita en primera persona y en estilo epistolar (como una sola y larga carta), cuya edicion conocida mis antigua data de 1554"));
        spout.setCycle(true);
        
        TridentTopology topology = new TridentTopology();        
        TridentState wordCounts =
              topology.newStream("spout", spout)
                .parallelismHint(3)
                .each(new Fields("text"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))         
                .parallelismHint(3);
                
        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        
        return topology.build();
    }
    
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        
		if (args.length == 0) {					        
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
			
			for(int i=0; i<2; i++) {
				Thread.sleep(1000);
				LOG.info("Es: " + drpc.execute("words", "Es"));
				LOG.info("una: " + drpc.execute("words", "una"));
				LOG.info("novela: " + drpc.execute("words", "novela"));
				LOG.info("primera:" + drpc.execute("words", "primera"));                		            
		    }
		} else {
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology("wordCounter", conf, buildTopology(null));
							
			//DRPCClient client = new DRPCClient("vmlbcnimbusl01", 3772);
			DRPCClient client = new DRPCClient("pivhdsne", 3772);
			
			for(int i=0; i<20; i++) {
				Thread.sleep(500);
				LOG.info("Es: " + client.execute("words", "Es"));
				LOG.info("una: " + client.execute("words", "una"));
				LOG.info("novela: " + client.execute("words", "novela"));
				LOG.info("primera: " + client.execute("words", "primera"));
			}
		}          
    }
}
