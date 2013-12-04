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

package com.produban.openbus.processor.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class QueryRequestDay extends BaseQueryFunction<ReadOnlyMapState, HashMap> {
    public List<HashMap> batchRetrieve(ReadOnlyMapState state, List<TridentTuple> inputs) {
        List<HashMap> ret = new ArrayList();
        Long i = new Long(0);
//        for(TridentTuple input: inputs) {
//        
//        	List lista = state.multiGet(inputs);
//        	i = i + 1;
//        	//ret.add("vale"); 
//        	
//            //ret.add(state.getLocation(input.getLong(0)));
//        }
        
        ret = state.multiGet((List) inputs);
        
        Map<String, Long> register = null;
        
        if (ret != null) { 
        	register = ret.get(0);
        	System.out.println("inputs.get(0) " + inputs.get(0));
        	if (register != null)  System.out.println("register.get(inputs.get(0)) " + register.get(inputs.get(0)));
        }
       
        
        
        //register.get(inputs.get(0));
        
        
        return ret;
    }

    public void execute(TridentTuple tuple, HashMap result, TridentCollector collector) {
        collector.emit(new Values(result));
    }    
}