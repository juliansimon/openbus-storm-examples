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
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

/**
 * The last operations of the user
 *
 */
public class UserOperationsLastList implements CombinerAggregator<LinkedList<Map<String, String>>> {

	private static final long serialVersionUID = 1L;	
	private static final int MAX_SIZE_USER_OPERATIONS = 3;

	public LinkedList<Map<String, String>> init(TridentTuple tuple) {
		LinkedList<Map<String, String>> linkedListMap = zero();
				
		TridentTupleView tridentTupleView = (TridentTupleView)tuple;
		tridentTupleView.getValues();
				
		Map<String, String> map = new HashMap<String, String>();
		map.put("request", tuple.getStringByField("request"));
		map.put("datetime", tuple.getStringByField("datetime"));
		
		linkedListMap.add(map);
				
		return linkedListMap;
	}

	public LinkedList<Map<String, String>> combine(LinkedList<Map<String, String>>val1, LinkedList<Map<String, String>> val2) {			
		ListIterator<Map<String, String>> itr = val1.listIterator();
				
	    while(itr.hasNext()) {
	    	Map<String, String> map = itr.next();	    	
	    	val2.add(map);
	    	
	    	if (val2.size() > MAX_SIZE_USER_OPERATIONS) {
	    		val2.removeLast();
	    		val1.removeLast();
	    	}	    	
	    }
				
		return val2;
	}

	public LinkedList<Map<String, String>> zero() {
		return new LinkedList<Map<String, String>>();
	}
}
