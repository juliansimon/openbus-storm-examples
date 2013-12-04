package com.produban.openbus.processor.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class WSLogPartition extends BaseFunction {
	private static final long serialVersionUID = 9124023812950937246L;
	private static final Logger LOG = LoggerFactory.getLogger(WSLogPartition.class);
			
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {    
        	 collector.emit(new Values("host", tuple.getStringByField("host")));
        	 collector.emit(new Values("log", tuple.getStringByField("log")));
        	 collector.emit(new Values("user", tuple.getStringByField("user")));
        	 collector.emit(new Values("datetime", tuple.getStringByField("datetime")));
        	 collector.emit(new Values("request", tuple.getStringByField("request")));
        	 collector.emit(new Values("status", tuple.getStringByField("status")));
        	 collector.emit(new Values("size", tuple.getStringByField("size")));
        	 collector.emit(new Values("referer", tuple.getStringByField("referer")));
        	 collector.emit(new Values("userAgent", tuple.getStringByField("userAgent")));
        	 collector.emit(new Values("session", tuple.getStringByField("session")));
        	 collector.emit(new Values("responseTime", tuple.getStringByField("responseTime")));
        	 collector.emit(new Values("timestamp", tuple.getStringByField("timestamp")));
        	 collector.emit(new Values("json", tuple.getStringByField("json")));              
        } catch (Exception e) {
        	LOG.error("Format timestamp hour: " + tuple.getString(0) + " " + tuple.getString(1) + " ", e); 
		} 
    }
}