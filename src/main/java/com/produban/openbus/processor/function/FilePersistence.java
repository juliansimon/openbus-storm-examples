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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.utils.Time;

import com.produban.openbus.processor.hdfs.HDFSStore;

/**
 * Function Storm/Trident for persistence in file   
 */
public class FilePersistence extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(FilePersistence.class);
	private static final long serialVersionUID = 1L;
	
	private DataFileWriter<GenericRecord> dataFileWriter;
	private Schema schema;
	private long cacheCont; 	
	private int cacheSize = 1000;
	private String schemaAvro = null;
	private Map<String, String> configuration;
	private String pathFile = null;
	private HDFSStore hDFSStore;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		configuration = conf;	
		schemaAvro = (String) conf.get("openbus.schemaAvro");		
		if (conf.get("openbus.cacheSize") != null && !"".equals(conf.get("openbus.cacheSize"))) {
			cacheSize = Integer.parseInt((String)conf.get("openbus.cacheSize"));
		}

		hDFSStore = new HDFSStore();		
		dataFileWriter = getDataFileWriter();
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {			
		// Cache 
		if (cacheCont > cacheSize) {			
			cleanup();			
			setFileinRepository(pathFile);	
			deleteFile(pathFile);
			dataFileWriter = getDataFileWriter();	
		}
		cacheCont ++;
		
		setTupleInFile(tuple);		
	}
	
	@Override
	public void cleanup() {
		try {
			dataFileWriter.close();
			cacheCont = 0;
		} 
		catch (IOException e) {
			LOG.error("Error Closing file: " + e);
		}
	}
	
	private DataFileWriter<GenericRecord> getDataFileWriter() {		
		DataFileWriter<GenericRecord> dataFileWriter;
		
		try {		
			pathFile = (String) configuration.get("openbus.documentPath") + "-" + System.nanoTime();			
			Schema.Parser p = new Schema.Parser();
			schema = p.parse(FilePersistence.class.getResourceAsStream(schemaAvro));			
			File file = new File(pathFile);
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			if(file.exists())
				dataFileWriter.appendTo(file);
			else 
				dataFileWriter.create(schema, file);						
		} 
		catch (IOException e) {
			throw new RuntimeException(e);
		}		
		
		return dataFileWriter;		
	}
	
	private void setTupleInFile(TridentTuple tuple) {	
		GenericRecord docEntry = new GenericData.Record(schema);						
		docEntry.put("docid", tuple.getStringByField("text"));
		docEntry.put("time", Time.currentTimeMillis());
		docEntry.put("line", tuple.getStringByField("text"));
		
		try {
			dataFileWriter.append(docEntry);
			dataFileWriter.flush();
		} catch (IOException e) {
			LOG.error("Error writing to document record: " + e);
			throw new RuntimeException(e);
		}		
	}

	private void setFileinRepository(String file) {		
		try {							
			hDFSStore.copyFromLocalFile(file, (String)configuration.get("openbus.hdfsDir") + file);
		}
		catch (IOException e) {
			LOG.error("Error Persistence file in HDFS: " + e);
			throw new RuntimeException(e);
		}		
	}
	
	private boolean deleteFile(String pathFile) {
		boolean success = false;
		File file = new File(pathFile);
		
		if (file.exists()) { 
			success = file.delete();
		}
		
		if (!success) {
		     throw new IllegalArgumentException("Delete: deletion failed " + pathFile);
		}
		
		return success;
	}
	
	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}
	
}
