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

import java.util.HashMap;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class SumMap implements CombinerAggregator<HashMap> {

    @Override
    public HashMap init(TridentTuple tuple) {
    	
    	HashMap hashMap = (HashMap) tuple.getValue(0);
    	
        return hashMap;
    }

    @Override
    public HashMap combine(HashMap val1, HashMap val2) {
    	//String num2 = (String)val2.get("2012091919");
    	//String num1 = (String)val1.get("2012091919");
    	    	
    	HashMap hashMap = new HashMap();
    	
        return val2;
    }

    @Override
    public HashMap zero() {
        return new HashMap();
    }
    
}

