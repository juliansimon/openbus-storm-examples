package com.produban.openbus.processor.util;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class CountField implements CombinerAggregator<Long> {
	private static final long serialVersionUID = 8121106874124749639L;

	@Override
	public Long init(TridentTuple tuple) {
		return new Long(tuple.getString(2));		
	}

	@Override
	public Long combine(Long val1, Long val2) {
		return val1 + val2;
	}

	@Override
	public Long zero() {
		return 0L;
	}
}

