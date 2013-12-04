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
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.produban.openbus.processor.filter.WebServerLogFilter;
import com.produban.openbus.processor.function.DateTimeInDay;
import com.produban.openbus.processor.function.DateTimeInHour;
import com.produban.openbus.processor.function.DateTimeInMonth;
import com.produban.openbus.processor.function.ExpandFields;
import com.produban.openbus.processor.function.OpenbusAvroLogDecoder;
import com.produban.openbus.processor.function.WebServerLog2JsonTS;
import com.produban.openbus.processor.properties.Conf;
import com.produban.openbus.processor.spout.OpenbusBrokerSpout;
import com.produban.openbus.processor.spout.SimpleFileStringSpout;
import com.produban.openbus.processor.util.CountField;
import com.produban.openbus.processor.util.LogFilter;

public class OpenbusWebServerLogTopology {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusWebServerLogTopology.class);
		    
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
		
	    // Get spout
	    if(Conf.BROKER_SPOUT) {
			OpenbusBrokerSpout openbusBrokerSpout = new OpenbusBrokerSpout(
					(String)conf.get(Conf.PROP_BROKER_TOPIC), 
					(String)conf.get(Conf.PROP_ZOOKEEPER_HOST), 
					(String)conf.get(Conf.PROP_ZOOKEEPER_BROKER));
				
			stream = topology.newStream("spout", openbusBrokerSpout.getPartitionedTridentSpout());			
			stream = stream.each(new Fields("bytes"), new OpenbusAvroLogDecoder(), new Fields(fieldsWebLog));
	    }
	    else {
	      SimpleFileStringSpout spout = new SimpleFileStringSpout("data/webapplogsNewFormat.json", "rawLogs");
	      spout.setCycle(true);

	      stream = topology.newStream("spout", spout);
	      stream = stream.each(new Fields("rawLogs"), new WebServerLog2JsonTS(), new Fields(fieldsWebLog));
	    }
	
		// Filter
		stream = stream.each(new Fields(fieldsWebLog), new WebServerLogFilter());

		// Aggregates by hour request
		Stream streamRequest = stream.each(new Fields("request", "datetime"), new DateTimeInHour(), new Fields("field_hour"))				
				.groupBy(new Fields("field_hour"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))					
				.newValuesStream()
				.each(new Fields("field_hour", "count"), new LogFilter());

		// Aggregate day request
		streamRequest = streamRequest.each(new Fields("field_hour", "count"), new ExpandFields(), new Fields("field", "hour", "count_by_hour"));
		streamRequest = streamRequest.each(new Fields("field", "hour", "count_by_hour"), new DateTimeInDay(), new Fields("field_day", "hour_day"))				
				.groupBy(new Fields("field_day"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("field_day", "hour", "count_by_hour"), new CountField(), new Fields("count_day"))					
				.newValuesStream()
				.each(new Fields("field_day", "count_day"), new LogFilter());
		
		// Aggregate month request
		streamRequest = streamRequest.each(new Fields("field_day", "count_day"), new ExpandFields(), new Fields("field", "day", "count_by_day"));
		streamRequest = streamRequest.each(new Fields("field", "day", "count_by_day"), new DateTimeInMonth(), new Fields("field_month", "month"))				
				.groupBy(new Fields("field_month"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("field_month", "day", "count_by_day"), new CountField(), new Fields("count_month"))					
				.newValuesStream()
				.each(new Fields("field_month", "count_month"), new LogFilter());

		// Aggregates by hour user
		Stream streamUser = stream.each(new Fields("user", "datetime"), new DateTimeInHour(), new Fields("field_hour"))				
				.groupBy(new Fields("field_hour"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))					
				.newValuesStream()
				.each(new Fields("field_hour", "count"), new LogFilter());

		// Aggregate by day user
		streamUser = streamUser.each(new Fields("field_hour", "count"), new ExpandFields(), new Fields("field", "hour", "count_by_hour"));
		streamUser = streamUser.each(new Fields("field", "hour", "count_by_hour"), new DateTimeInDay(), new Fields("field_day", "hour_day"))				
				.groupBy(new Fields("field_day"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("field_day", "hour", "count_by_hour"), new CountField(), new Fields("count_day"))					
				.newValuesStream()
				.each(new Fields("field_day", "count_day"), new LogFilter());

		// Aggregate by month user
		streamUser = streamUser.each(new Fields("field_day", "count_day"), new ExpandFields(), new Fields("field", "day", "count_by_day"));
		streamUser = streamUser.each(new Fields("field", "day", "count_by_day"), new DateTimeInMonth(), new Fields("field_month", "month"))				
				.groupBy(new Fields("field_month"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("field_month", "day", "count_by_day"), new CountField(), new Fields("count_month"))					
				.newValuesStream()
				.each(new Fields("field_month", "count_month"), new LogFilter());		
													
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