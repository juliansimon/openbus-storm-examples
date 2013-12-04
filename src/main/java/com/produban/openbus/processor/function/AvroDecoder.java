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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.produban.openbus.processor.properties.Conf;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * Function Storm/Trident for Decoder Avro  
 */
public class AvroDecoder extends BaseFunction {
	Logger LOG = LoggerFactory.getLogger(AvroDecoder.class);
	
	private static final long serialVersionUID = 1L;
	
	private Schema schema;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {		
		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(getClass().getClassLoader().getResourceAsStream(Conf.SCHEMA_APACHE_LOGS));
		} catch (IOException e) {
			LOG.error("Parser schema avro " + e);
			throw new RuntimeException(e);
		}		
	}

	@Override
	public final void execute(final TridentTuple tuple, final TridentCollector collector) {
		GenericRecord result = null;
		int _offset = 14;		
		byte[] bytes = (byte[]) tuple.get(0);
				
		try {									
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);			
		    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, _offset, bytes.length - _offset, null);		    		    
		    result = reader.read(null, decoder);
		    
		    LOG.info("Message Avro: " + result.toString());	
		    
		    collector.emit(new Values(result)); 
		} catch (UnsupportedEncodingException e) {
			LOG.error("Avro message format not supported " + e);
			throw new RuntimeException(e);
		} catch (IOException e) {
			LOG.error("Avro message error in IO " + e);
			throw new RuntimeException(e);
		} catch (Exception e) {
			LOG.error("Error in Avro message " + e);
			throw new RuntimeException(e);
		}		
	}
}
