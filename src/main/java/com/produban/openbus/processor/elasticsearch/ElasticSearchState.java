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
package com.produban.openbus.processor.elasticsearch;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import com.produban.openbus.processor.properties.Conf;

/**
 * State for Elastic Search
 * 
 */
public class ElasticSearchState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchState.class);

    private Client client;

    @SuppressWarnings("rawtypes")
    public ElasticSearchState(Map conf) {        
        String cluster = (String)conf.get(Conf.PROP_ELASTICSEARCH_CLUSTER);
        String host = (String)conf.get(Conf.PROP_ELASTICSEARCH_HOST);
        int port = new Integer((String)conf.get(Conf.PROP_ELASTICSEARCH_PORT)).intValue();

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, port));
        
        if (LOG.isDebugEnabled()) {
        	LOG.debug(Conf.PROP_ELASTICSEARCH_CLUSTER + " : " + cluster 
        			+ "  " + Conf.PROP_ELASTICSEARCH_HOST + " : " + host 
        			+ " "  + Conf.PROP_ELASTICSEARCH_PORT + " : " + port);
        }
    }

    public ElasticSearchState(Client client) {
        this.client = client;
    }

    @Override
    public void beginCommit(Long txId) {
    	if (LOG.isDebugEnabled())  LOG.debug("beginCommit: " + txId);
    }

    @Override
    public void commit(Long txId) {
    	if (LOG.isDebugEnabled())  LOG.debug("txId: " + txId);
    }

    public void addDocuments(List<TridentTuple> tuples) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        String jsonString = null;
        
        try {
        	for (TridentTuple tuple : tuples) {
        		jsonString = tuple.getStringByField("json");
        		UUID id = UUID.randomUUID();    	    	                        
       	 		bulkRequest.add(client.prepareIndex(Conf.ELASTICSEARCH_INDEX_NAME_WEBLOG, Conf.ELASTICSEARCH_INDEX_TYPE_WEBLOG,
       	 				id.toString()).setSource(jsonString));
       	 	
       	 		if (LOG.isDebugEnabled()) LOG.debug("Document to ElasticSearch: " + jsonString);       	 	

       	 		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
       	 		if (bulkResponse.hasFailures()) {
       	 			LOG.error("Not execute or actionGet: " + jsonString + " "+ bulkResponse.buildFailureMessage());
       	 		}
        	}
        } 
        catch (Exception e) {
        	LOG.error("Error document persist in ElasticSearch "  + jsonString, e);
        }
    }
}