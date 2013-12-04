package com.produban.openbus.processor.function;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import com.produban.openbus.processor.util.FormatUtil;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class OpenbusAvroDecoder extends BaseFunction {

	private static Logger LOG = LoggerFactory.getLogger(OpenbusAvroDecoder.class);    
	private static final long serialVersionUID = 1L;
    
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {        
        
    		    	    	
    	try { 
        	byte[] temp = tuple.getBinary(0);
        	byte[] payload=new byte[temp.length - 14];
        	System.arraycopy(temp, 14, payload, 0, temp.length - 14);
         	
            DatumReader<Record> reader = new GenericDatumReader<Record>();
            ByteArrayInputStream is = new ByteArrayInputStream(payload);
            DataFileStream<Record> dataFileReader;
        	
			dataFileReader = new DataFileStream<Record>(is, reader);

	    	Record record=null;
	    	
	        while (dataFileReader.hasNext()) {
	            record = dataFileReader.next(record);
	        }
	        

	        dataFileReader.close();
      
              String request = FormatUtil.getRequestFormat(record.get("request").toString());
              long datetime = FormatUtil.getDateInFormat10Second(record.get("datetime").toString());
              collector.emit(new Values(record.get("user").toString(), request, String.valueOf(datetime)));
              
        } catch (Exception e) {
        	LOG.error("Caught DecoderException: " + e.getMessage()); 
        	//throw new RuntimeException(e);
		} 
    }
}
