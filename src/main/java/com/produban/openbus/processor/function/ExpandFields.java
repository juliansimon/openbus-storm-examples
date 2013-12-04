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

package com.produban.openbus.processor.function;


import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.produban.openbus.processor.properties.OpenbusConstant;
import com.produban.openbus.processor.util.FormatUtil;

/**
 * Function Storm/Trident for expand fields    
 */
public class ExpandFields extends BaseFunction {	
	private static final long serialVersionUID = -7323673364626780266L;
	private static Logger LOG = LoggerFactory.getLogger(ExpandFields.class);    
    	
	@Override
	@SuppressWarnings("rawtypes") 
	public void prepare(Map conf, TridentOperationContext context) {
	}
    
    @Override
    public final void execute(final TridentTuple tuple, final TridentCollector collector) {        
        try { 
        	int pos = tuple.getString(0).indexOf(OpenbusConstant.SEPARATOR_FIELDS);
        	String field1 = tuple.getString(0).substring(0, pos);
        	String field2 = tuple.getString(0).substring(pos + OpenbusConstant.SEPARATOR_FIELDS.length(), tuple.getString(0).length());
        	            
            if (LOG.isDebugEnabled()) {
            	LOG.debug("Tuple: " + tuple.getString(0));
            	LOG.debug("field1: " + field1);
            	LOG.debug("field2: " + field2);
            }
                     
            collector.emit(new Values(field1, field2, String.valueOf(tuple.getLong(1))));            
        } catch (Exception e) {
        	LOG.error("Expand field: " + tuple.getString(0) + " " + String.valueOf(tuple.getLong(1)), e); 
		} 
    }
}