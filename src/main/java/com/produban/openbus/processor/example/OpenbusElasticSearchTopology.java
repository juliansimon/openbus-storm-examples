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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.elasticsearch.ElasticSearchStateFactory;
import com.produban.openbus.processor.elasticsearch.ElasticSearchStateUpdater;
import com.produban.openbus.processor.filter.WebServerLogFilter;
import com.produban.openbus.processor.function.OpenbusAvroLogDecoder;
import com.produban.openbus.processor.properties.Conf;
import com.produban.openbus.processor.spout.OpenbusBrokerSpout;

/**
 * Example of topology integration Elastic Search
 * 
 */
public class OpenbusElasticSearchTopology {        
	private static Logger LOG = LoggerFactory.getLogger(OpenbusElasticSearchTopology.class);
                    
    public static StormTopology buildTopology(Config conf) {                        
        TridentTopology topology = new TridentTopology();
        Stream stream = null;
                        
        List<String> fieldsWebLog = new ArrayList<String>();
        fieldsWebLog.add("host");
        fieldsWebLog.add("log");
        fieldsWebLog.add("user");
        fieldsWebLog.add("datetime");
        fieldsWebLog.add("request");
        fieldsWebLog.add("status");
        fieldsWebLog.add("size");
        fieldsWebLog.add("referer");
        fieldsWebLog.add("userAgent");
        fieldsWebLog.add("session");
        fieldsWebLog.add("responseTime");
        fieldsWebLog.add("timestamp");
        fieldsWebLog.add("json");
                        
        OpenbusBrokerSpout openbusBrokerSpout = new OpenbusBrokerSpout(
        		(String)conf.get(Conf.PROP_BROKER_TOPIC), 
                (String)conf.get(Conf.PROP_ZOOKEEPER_HOST), 
                (String)conf.get(Conf.PROP_ZOOKEEPER_BROKER));
                
        stream = topology.newStream("spout", openbusBrokerSpout.getPartitionedTridentSpout());                        
        stream = stream.each(new Fields("bytes"), new OpenbusAvroLogDecoder(), new Fields(fieldsWebLog));
                                        
        // Filter
        stream = stream.each(new Fields(fieldsWebLog), new WebServerLogFilter());
                                                                                                
        // Index                
        TridentState elasticSearchState = stream.partitionPersist(
                new ElasticSearchStateFactory(), new Fields(fieldsWebLog), new ElasticSearchStateUpdater());
                                                
        return topology.build();                                
    }

    public static void main(String[] args) throws Exception {
    	Config conf = new Config();                
        //conf.setDebug(true);
        conf.put(Conf.PROP_BROKER_TOPIC, Conf.KAFKA_TOPIC);
        conf.put(Conf.PROP_ZOOKEEPER_HOST, Conf.ZOOKEEPER_HOST + ":" + Conf.ZOOKEEPER_PORT);
        conf.put(Conf.PROP_ZOOKEEPER_BROKER, Conf.ZOOKEEPER_BROKER);                
        conf.put(Conf.PROP_ELASTICSEARCH_CLUSTER, Conf.ELASTICSEARCH_CLUSTER);
        conf.put(Conf.PROP_ELASTICSEARCH_HOST, Conf.ELASTICSEARCH_HOST);
        conf.put(Conf.PROP_ELASTICSEARCH_PORT, Conf.ELASTICSEARCH_PORT);
                        
        if (args.length == 0) {                
        	LOG.info("Storm mode local");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("openbus", conf, buildTopology(conf));                        
            Thread.sleep(2000);                                                           
             //cluster.shutdown();
        } else {
        	LOG.info("Storm mode cluster");
        	StormSubmitter.submitTopology("openbus", conf, buildTopology(conf));
        	Thread.sleep(2000);                        
        }
    }        
}