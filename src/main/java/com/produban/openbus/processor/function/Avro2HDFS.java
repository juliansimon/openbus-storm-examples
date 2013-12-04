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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.produban.openbus.processor.hdfs.HDFSStore;

/**
 * Function Storm/Trident for persistence of a file Avro in HDFS 
 */
public class Avro2HDFS extends BaseFunction {
	Logger LOG = LoggerFactory.getLogger(Avro2HDFS.class);

	private static final long serialVersionUID = 1L;
	private static final String FILE_NAME_AVRO = "logs_avro"; // TODO: Properties
	
	private HDFSStore hDFSStore;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		hDFSStore = new HDFSStore();
	}

	@Override
	public final void execute(final TridentTuple tuple, final TridentCollector collector) {
		String strBytes = null;
		byte[] bytes = (byte[]) tuple.get(0);
		
		try {
			strBytes = new String(bytes, "UTF-8");
			hDFSStore.writeFile2HDFS(FILE_NAME_AVRO + "_" + System.nanoTime(), strBytes);
		} catch (UnsupportedEncodingException e) {
			LOG.error("Avro message format not supported " + e);
			throw new RuntimeException(e);
		} catch (IOException e) {
			LOG.error("Write file avro in repository " + e);
			throw new RuntimeException(e);
		}
	}
}
