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
package com.produban.openbus.processor.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.produban.openbus.processor.properties.Conf;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Filter Web Server log
 * 
 */
public class WebServerLogFilter extends BaseFilter {	
	private static Logger LOG = LoggerFactory.getLogger(WebServerLogFilter.class);
	private static final long serialVersionUID = 3184944296594510118L;
		
    @Override
    public boolean isKeep(TridentTuple tuple) {
        if (LOG.isDebugEnabled())  LOG.debug("Applying WebServerLogFilter");
        
        String host = (String)tuple.getStringByField("host");
        
        if (Conf.FILTER_HOST_WEBSERVER !=null && !"".equals(Conf.FILTER_HOST_WEBSERVER) 
        		&& !host.matches(Conf.FILTER_HOST_WEBSERVER)) {
        	if (LOG.isDebugEnabled())  LOG.debug("Filter by host " + Conf.FILTER_HOST_WEBSERVER + " host " + host);
        	return false;
        }

        if (Conf.FILTER_NOT_HOST_WEBSERVER !=null && !"".equals(Conf.FILTER_NOT_HOST_WEBSERVER) 
        		&& host.matches(Conf.FILTER_NOT_HOST_WEBSERVER)) {
        	if (LOG.isDebugEnabled())  LOG.debug("Filter not by host  " + Conf.FILTER_NOT_HOST_WEBSERVER + " host " + host);
        	return false;
        }
        
        String request = (String)tuple.getStringByField("request");               
        if (Conf.FILTER_REQUEST_WEBSERVER !=null && !"".equals(Conf.FILTER_REQUEST_WEBSERVER) 
        		&& !request.matches(Conf.FILTER_REQUEST_WEBSERVER)) {
        	if (LOG.isDebugEnabled())  LOG.debug("Filter by request " + Conf.FILTER_REQUEST_WEBSERVER + " request " + request);
        	return false;
        }
        
        if (Conf.FILTER_NOT_REQUEST_WEBSERVER !=null && !"".equals(Conf.FILTER_NOT_REQUEST_WEBSERVER) 
        		&& request.matches(Conf.FILTER_NOT_REQUEST_WEBSERVER)) {
        	if (LOG.isDebugEnabled())  LOG.debug("Filter not by request  " + Conf.FILTER_NOT_REQUEST_WEBSERVER + " request " + request);
        	return false;
        }
        
        String status = (String)tuple.getStringByField("status");
        if (Conf.FILTER_STATUS_WEBSERVER !=null && !"".equals(Conf.FILTER_STATUS_WEBSERVER) 
        		&& !status.matches(Conf.FILTER_STATUS_WEBSERVER)) {
        	if (LOG.isDebugEnabled())  LOG.debug("Filter by status " + Conf.FILTER_STATUS_WEBSERVER + " status " + status);
        	return false;
        }
        
        if (Conf.FILTER_NOT_STATUS_WEBSERVER !=null && !"".equals(Conf.FILTER_NOT_STATUS_WEBSERVER) 
        		&& status.matches(Conf.FILTER_NOT_STATUS_WEBSERVER)) {
        	if (LOG.isDebugEnabled())  LOG.debug("Filter not by status " + Conf.FILTER_NOT_STATUS_WEBSERVER + " status " + status);
        	return false;
        }
        
        return true;
    }
}