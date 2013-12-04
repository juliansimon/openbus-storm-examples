package com.produban.openbus.processor.aggregator;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;


public class RequestAggregator extends BaseAggregator<Map<String, Integer>> {

	private static final long serialVersionUID = 440881135621832616L;

	@Override
	public Map<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer>();
	}

	@Override
	public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
		String location = tuple.getString(0);
		val.put(location, MapUtils.getInteger(val, location, 0) + 1);
	}

	@Override
	public void complete(Map<String, Integer> val, TridentCollector collector) {
		collector.emit(new Values(val));
	}
}
