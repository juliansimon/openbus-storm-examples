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
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.filter.WebServerLogFilter;
import com.produban.openbus.processor.function.OpenbusAvroLogDecoder;
import com.produban.openbus.processor.properties.Conf;
import com.produban.openbus.processor.spout.OpenbusBrokerSpout;
import com.produban.openbus.processor.util.RequestLog2TSDB;

/**
 * Example of topology integration OpenTSDB
 * 
 */
public class OpenbusOpenTSDBTopology {        
	private static Logger LOG = LoggerFactory.getLogger(OpenbusOpenTSDBTopology.class);
                    
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
                                                                                                
        // Send OpenTSDB (HBase)                                
        stream.groupBy(new Fields(fieldsWebLog)).aggregate(new Fields(fieldsWebLog), new RequestLog2TSDB(), new Fields("count"));
                                                                
        return topology.build();                                
    }

    public static void main(String[] args) throws Exception {
    	Config conf = new Config();                
        //conf.setDebug(true);
        conf.put(Conf.PROP_BROKER_TOPIC, Conf.KAFKA_TOPIC);
        conf.put(Conf.PROP_ZOOKEEPER_HOST, Conf.ZOOKEEPER_HOST + ":" + Conf.ZOOKEEPER_PORT);
        conf.put(Conf.PROP_ZOOKEEPER_BROKER, Conf.ZOOKEEPER_BROKER);                
                        
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