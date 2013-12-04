package com.produban.openbus.processor.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.Partition;
import storm.kafka.StaticHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.spout.IPartitionedTridentSpout;

public class OpenbusKafkaBrokerSpout {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusKafkaBrokerSpout.class);
	
	private final static String KAFKA_TOPIC = "tsdb1";
	private final static String ZOOKEEPERS_HOST = "pivhdsne:2181";
	private final static String ZOOKEEPERS_BROKER = "/brokers"; 
	//private final static String SERVER_IP = "192.168.20.136";
	
	
	private TridentKafkaConfig config = null;
	private ZkHosts zhost = null; 
	private IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> partitionedTridentSpout = null;


	public OpenbusKafkaBrokerSpout() {
    	zhost = new ZkHosts(ZOOKEEPERS_HOST, ZOOKEEPERS_BROKER);        
        config = new TridentKafkaConfig(zhost, KAFKA_TOPIC,"c-id-1");      
        //conf.put(Config.NIMBUS_HOST, );
        //config.forceStartOffsetTime(-1);
	}
	
	public OpenbusKafkaBrokerSpout(String kafkaTopic) {    	
    	zhost = new ZkHosts(ZOOKEEPERS_HOST, ZOOKEEPERS_BROKER);
    	
    	//StaticHosts staticHosts = new StaticHosts()
    	
        config = new TridentKafkaConfig(zhost, kafkaTopic,"c-id-1");                        
        //config.forceStartOffsetTime(-2);
  
	}
	
	public IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> getPartitionedTridentSpout() {		
		partitionedTridentSpout = new TransactionalTridentKafkaSpout(config);
		
		return partitionedTridentSpout;
	}
}
