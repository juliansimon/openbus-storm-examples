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

package com.produban.openbus.processor.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Count by day
 * 
 */
public class CountByDay implements CombinerAggregator<Map<String, Long>> {

	private static final long serialVersionUID = 1L;

	public Map<String, Long> init(TridentTuple tuple) {
		Map<String, Long> map = zero();
		map.put(tuple.getString(1), 1L);
		return map;
	}

	public Map<String, Long> combine(Map<String, Long> val1, Map<String, Long> val2) {
		for(Map.Entry<String, Long> entry : val2.entrySet()) {
			val2.put(entry.getKey(), MapUtils.getLong(val1, entry.getKey(), 0L) + entry.getValue());
		}
		for(Map.Entry<String, Long> entry : val1.entrySet()) {
			if(val2.containsKey(entry.getKey())) {
				continue;
			}
			val2.put(entry.getKey(), entry.getValue());
		}
		return val2;
	}

	public Map<String, Long> zero() {
		return new HashMap<String, Long>();
	}
}
