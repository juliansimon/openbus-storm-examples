package com.produban.openbus.processor.function;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.produban.openbus.processor.util.FormatUtil;

/**
 * Avro message decoder to json for web server log 
 * 
 */
public class OpenbusAvroLogDecoder extends BaseFunction {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusAvroLogDecoder.class);
	private static final long serialVersionUID = -8661808311962745765L;
        
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {                    		    	    	
    	try { 
    		byte[] temp = tuple.getBinary(0);
        	byte[] payload = new byte[temp.length - 14];
        	System.arraycopy(temp, 14, payload, 0, temp.length - 14);
         	
            DatumReader<Record> reader = new GenericDatumReader<Record>();
            ByteArrayInputStream is = new ByteArrayInputStream(payload);
            DataFileStream<Record> dataFileReader;        			
            dataFileReader = new DataFileStream<Record>(is, reader);
			Record record = null;
	    	
	        while (dataFileReader.hasNext()) {
	            record = dataFileReader.next(record);
	        }	        
			dataFileReader.close();
			
			String datetime = record.get("datetime").toString();
			long timestamp = FormatUtil.getDateInFormatTimeStamp(datetime);
			
			//TODO: Revisar el escapado de los tags ejemplo :
			collector.emit(new Values(record.get("host").toString(), 
					record.get("log").toString(), 
					record.get("user").toString(),  
					datetime,
					FormatUtil.getRequestFormat(record.get("request").toString()), 
					record.get("status").toString(), 
					record.get("size").toString(), 
					record.get("referer").toString(), 
					record.get("userAgent").toString(), 
					record.get("session").toString().replace(":", "_"), 
					record.get("responseTime").toString(), 
					String.valueOf(timestamp),
					record.toString().substring(0, record.toString().length() -1) 
						+ ", \"timestamp\": " + "\"" + String.valueOf(timestamp) + "\"}"));						
    	} catch (IOException e) {    
    		LOG.error("Decoder Avro IOException: ", e); 
    	} catch (Exception e) {
        	LOG.error("Avro Decoder Exception: ", e); 
		} 
    }
}
