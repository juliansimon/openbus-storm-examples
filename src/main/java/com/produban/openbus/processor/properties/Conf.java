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
package com.produban.openbus.processor.properties;

public interface Conf {
	public final static String PROP_ZOOKEEPER_HOST = "zookeper.host";
	public final static String PROP_ZOOKEEPER_BROKER = "zookeper.broker";
	
	public final static String ZOOKEEPER_HOST = "pivhdsne";
	public final static String ZOOKEEPER_PORT = "2181";
	public final static String ZOOKEEPER_BROKER = "/brokers";
	
	public final static String PROP_KAFKA_IDCLIENT = "kafka.idClient";
	public final static String KAFKA_IDCLIENT = "idOpenbus";
	public final static String KAFKA_TOPIC = "jsonTopic";
	public final static String KAFKA_BROKER_PORT = "9092"; 	
	
	
	public static final String HOST_OPENTSDB = "pivhdsne";
	public static final int PORT_OPENTSDB = 4242;
	public static final String SCHEMA_APACHE_LOGS = "apacheLog.avsc";
	
	public static final long TIME_PERIOD_MAX_RESPONSETIME = 5000; // Miliseconds
	public static final long TIME_PERIOD_REQUESTCOUNT = 5000; // Miliseconds
		 
	public final static String HDFS_DIR = "hdfs://pivhdsne:8020/user/gpadmin/openbus";
	public final static String HDFS_USER = "gpadmin";
	
	public final static String PROP_BROKER_TOPIC = "broker.topic";
	public static final String PROP_ELASTICSEARCH_CLUSTER = "elasticsearch.cluster";
	public static final String PROP_ELASTICSEARCH_HOST = "elasticsearch.host";
	public static final String PROP_ELASTICSEARCH_PORT = "elasticsearch.port";
		
	public static final String ELASTICSEARCH_CLUSTER = "elasticsearch";
	public static final String ELASTICSEARCH_HOST = "pivhdsne";
	public static final String ELASTICSEARCH_PORT = "9300";
		
	public final static String ELASTICSEARCH_INDEX_NAME_WEBLOG = "indexweblog_20131113";
	public final static String ELASTICSEARCH_INDEX_TYPE_WEBLOG = "typeweblog";
		
	public final static String FILTER_HOST_WEBSERVER = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
	public final static String FILTER_NOT_HOST_WEBSERVER = "";
	public final static String FILTER_REQUEST_WEBSERVER = "";
	public final static String FILTER_NOT_REQUEST_WEBSERVER = ".*\\.(png|jpg|css|js|bmp|gif)";	
	public final static String FILTER_STATUS_WEBSERVER = "^(200)";
	public final static String FILTER_NOT_STATUS_WEBSERVER = "";
	
	public final static String PROP_HBASE_TABLE_REQUEST = "hbase.table.request";
	public final static String PROP_HBASE_ROWID_REQUEST = "hbase.rowid.request";
	public final static String PROP_HBASE_ROWID_USER = "hbase.rowid.user";
	public final static String HBASE_TABLE_REQUEST = "wslog_request";
	public final static String PROP_HBASE_TABLE_USER = "hbase.table.user";
	public final static String HBASE_TABLE_USER = "wslog_user";
	public final static String HBASE_ROWID_REQUEST = "request";
	public final static String HBASE_ROWID_USER = "user";
			
	public final static String PROP_ELASTICSEARCH_USE = "elasticsearch";
	public final static String ELASTICSEARCH_USE = "yes";
	public final static String PROP_OPENTSDB_USE = "opentsdb";
	public final static String OPENTSDB_USE = "yes";
	public final static String PROP_HDFS_USE = "hdfs";
	public final static String HDFS_USE = "no";
	
	/* Options */
	public final static boolean BROKER_SPOUT = true;
}

