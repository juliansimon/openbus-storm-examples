package com.produban.openbus.processor.util;



import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import com.produban.openbus.processor.register.OpenTSDBRecoder;
import com.produban.openbus.processor.register.RemoteRecoder;
import com.produban.openbus.processor.util.CountTSDB.CountState;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class CountTSDB implements Aggregator<CountState> {
	
    static class CountState {
        long count = 0;
        String user;
        String request;
        String datetime;
        RemoteRecoder recoder;
    }

    public CountState init(Object batchId, TridentCollector collector) {
        
    	RemoteRecoder recorder = new OpenTSDBRecoder();
        try {
			recorder.init("pivhdsne", 4242);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        CountState cs = new CountState();
        cs.recoder=recorder;
        return cs;
    }

    public void aggregate(CountState state, TridentTuple tuple, TridentCollector collector) {
        state.count+=1;
        state.user=tuple.getString(0);
        state.request=tuple.getString(1);
        state.datetime=tuple.getString(2);
    }

    public void complete(CountState state, TridentCollector collector) {
    	if(state.count>0) {
    		//System.out.println("*******complete ***** time: " + new Date());
    		//System.out.println("##### val1: " + state.count);    	    		    		
    		String metric = "put apache.requests " + state.datetime + " " +  state.count + " user=" + state.user + " request=" + state.request + "\n";       
    		try {
				state.recoder.send(metric);
		        state.recoder.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
  
    		System.out.println(metric);
    		collector.emit(new Values(state.count));
    	}
    }

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
}

