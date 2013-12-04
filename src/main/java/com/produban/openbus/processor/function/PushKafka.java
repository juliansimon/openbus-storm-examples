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

package com.produban.openbus.processor.function;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.produban.openbus.processor.properties.Conf;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * Function Storm/Trident push to Kafka    
 */
public class PushKafka extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(PushKafka.class);
	private static final long serialVersionUID = 1L;	
	private final static String KAFKA_TOPIC_SEND = "test1";
	 
	protected Producer<String, String> producer;
	ProducerConfig config;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		Properties props = new Properties();
		props.put("zk.connect", Conf.ZOOKEEPER_HOST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");		
		props.put("metadata.broker.list", Conf.ZOOKEEPER_HOST + ":" + Conf.KAFKA_BROKER_PORT);
		
		config = new ProducerConfig(props);
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {		
		byte[] bytes = tuple.getBinary(0);
		String mesg = new String(bytes);        
		producer = new Producer<String, String>(config);
		KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(KAFKA_TOPIC_SEND, mesg + "_end");
		producer.send(keyedMessage);
	}	
}