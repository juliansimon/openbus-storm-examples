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

package com.produban.openbus.processor.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Spout File 
 *
 */
public class SimpleFileStringSpout extends BaseRichSpout {

	private final String emittedTupleName;

	private static BufferedReader br;
	private SpoutOutputCollector collector;
	private boolean cycle = false;
	private String fileName;    
	
	public void setCycle(boolean cycle) {
	        this.cycle = cycle;
	}
		
	public SimpleFileStringSpout(String sourceFileName, String emittedTupleName) {
		super();
		this.emittedTupleName = emittedTupleName;
		try {
			br = new BufferedReader(new FileReader(new File(sourceFileName)));
			fileName = sourceFileName;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void close() {
		if (br != null) {
			try {
				br.close();
				br = null;
			} catch (IOException e) {
				System.err.println("could not close input stream to json file containing events.");
				e.printStackTrace();
			}
		}
	}

	public void nextTuple() {
		try {
			String rawEvent = br.readLine();
			if (rawEvent != null) {
				try {
					String messageId = UUID.randomUUID().toString();
					collector.emit(new Values(rawEvent), messageId);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (cycle) {
				br.close();
				br = new BufferedReader(new FileReader(new File(fileName)));				
			} else {	
				Utils.sleep(1);
			}
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(emittedTupleName));
	}

	@Override
	public void ack(Object msgId) {
		// yeah
	}

	@Override
	public void fail(Object msgId) {
		System.err.println("oups: failed message: " + msgId + ", not retrying");
	}

}
