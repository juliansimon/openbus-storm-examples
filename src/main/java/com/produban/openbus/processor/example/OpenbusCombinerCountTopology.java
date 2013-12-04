package com.produban.openbus.processor.example;

import com.produban.openbus.processor.function.OpenbusAvroDecoder;
import com.produban.openbus.processor.spout.OpenbusKafkaBrokerSpout;
import com.produban.openbus.processor.util.CountTSDB;
import com.produban.openbus.processor.util.LogFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class OpenbusCombinerCountTopology {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusCombinerCountTopology.class);
		    
	public static StormTopology buildTopology(LocalDRPC drpc) {	
				
		TridentTopology topology = new TridentTopology();
		 
		//Stream termStream = topology.newStream("spout", new SimpleFileStringSpout("data/webapplogs.json", "rawLogs"));
		OpenbusKafkaBrokerSpout openbusBrokerSpout = new OpenbusKafkaBrokerSpout();
		
		Stream termStream = topology.newStream("spout1", openbusBrokerSpout.getPartitionedTridentSpout());

		//TridentState wordCounts = termStream
//				.each(new Fields("bytes"), new ParseJSONLogUserRequest(), new Fields("user", "request", "datetime"))
//        		.groupBy(new Fields("user","request","datetime"))//,"request"))
//        		.persistentAggregate(new MemoryMapState.Factory(), new CountConsole(), new Fields("count"));
        		//.each(new Fields("user", "request", "count"), new LogFilter());
		termStream
		.each(new Fields("bytes"), new OpenbusAvroDecoder(), new Fields("user", "request", "datetime"))
		.groupBy(new Fields("user","request","datetime"))
		.aggregate( new Fields("user", "request", "datetime"),new CountTSDB(), new Fields("count"))
		//.persistentAggregate(new MemoryMapState.Factory(), new Fields("user","request","datetime"),new CountTSDB(), new Fields("count"));
		.each(new Fields("count"), new LogFilter());		
	        		//.parallelismHint(3);
			
		//wordCounts.newValuesStream().each(new Fields("user", "request", "datetime", "count"), new LogFilter());
			
		
//        topology.newDRPCStream("words", drpc)
//        .each(new Fields("args"), new SplitUserRequest(), new Fields("user","request","datetime"))
//        .groupBy(new Fields("user","request","datetime"))
//        .stateQuery(wordCounts, new Fields("user","request","datetime"), new MapGet(), new Fields("count"))
//        .each(new Fields("count"), new FilterNull())
//        .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
//		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(1);		
		
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        

		
		
		if (args.length == 0) {		
			LOG.debug("Storm mode local");
//			LocalDRPC drpc = new LocalDRPC();
//			LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("AvroDecoder", conf, buildTopology(drpc));	
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
			//TODO: Create the twitter spout and pass it in here...
			//StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
		
		
        for(int i=0; i<100; i++) {
        	Thread.sleep(1000);
            System.out.println("Nº de ocurrencias de user820: " + drpc.execute("words", "user820 index.html 20120919200526"));
            System.out.println("Nº de ocurrencias de user819: " + drpc.execute("words", "user819 index.html 20120919200526"));
            System.out.println("Nº de ocurrencias de user821: " + drpc.execute("words", "user821 index.html 20120919200526"));
            System.out.println("Nº de ocurrencias de user822: " + drpc.execute("words", "user822 index.html 20120919200526"));                
            //Thread.sleep(1000);
        }
		
	}		
}