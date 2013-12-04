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

package com.produban.openbus.processor.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;

/**
 * Basic example for test the cluster of storm
 */
public class TridentSocialNetworkTest {
	
	private static Logger LOG = LoggerFactory.getLogger(TridentSocialNetworkTest.class);
	
	public static Map<String, List<String>> WRITERS_DB = new HashMap<String, List<String>>() {{
			put("tuenti.com", Arrays.asList("sara", "juan", "robin", "jorge", "john"));
			put("slashdot.org", Arrays.asList("tintin", "david", "sara", "john"));
			put("tech.backtype.com/blog/123", Arrays.asList("robin", "mike", "john"));
			put("googleplus", Arrays.asList("pirlo", "taro"));
	}};

	public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
			put("sara", Arrays.asList("juan", "robin", "alice", "tintin", "spook", "nestor", "jai"));
			put("juan", Arrays.asList("sara", "john", "spook", "milu", "david", "laszlo"));
			put("robin", Arrays.asList("alex"));
			put("john", Arrays.asList("sara", "juan", "tintin", "harry", "nestor", "laszlo", "emily", "bianca"));
			put("tintin", Arrays.asList("david", "carissa"));
			put("mike", Arrays.asList("john", "juan"));
			put("john", Arrays.asList("alice", "john", "spook", "mike", "juan"));
			put("pirlo", Arrays.asList("buffon"));
			put("taro", Arrays.asList("maldini"));
	}};

	public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
		public static class Factory implements StateFactory {
			Map _map;

			public Factory(Map map) {
				_map = map;
			}

			@Override
			public State makeState(Map conf, IMetricsContext metrics,
					int partitionIndex, int numPartitions) {
				return new StaticSingleKeyMapState(_map);
			}
		}

		Map _map;

		public StaticSingleKeyMapState(Map map) {
			_map = map;
		}

		@Override
		public List<Object> multiGet(List<List<Object>> keys) {
			List<Object> ret = new ArrayList();
			for (List<Object> key : keys) {
				Object singleKey = key.get(0);
				ret.add(_map.get(singleKey));
			}
			
			return ret;
		}
	}

	public static class One implements CombinerAggregator<Integer> {
		@Override
		public Integer init(TridentTuple tuple) {
			return 1;
		}

		@Override
		public Integer combine(Integer val1, Integer val2) {
			return 1;
		}

		@Override
		public Integer zero() {
			return 1;
		}
	}

	public static class SpreadList extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			List l = (List) tuple.getValue(0);
			if (l != null) {
				for (Object o : l) {
					collector.emit(new Values(o));
				}
			}
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();
		TridentState urlToWriters = topology.newStaticState(new StaticSingleKeyMapState.Factory(WRITERS_DB));
		TridentState writersToFollowers = topology.newStaticState(new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));

		topology.newDRPCStream("scope", drpc)
				.stateQuery(urlToWriters, new Fields("args"), new MapGet(), new Fields("writers")) // get writers by url
				.each(new Fields("writers"), new SpreadList(), new Fields("writer")) // get writer 
				.shuffle()
				.stateQuery(writersToFollowers, new Fields("writer"), new MapGet(), new Fields("followers")) // get followers by writer
				.each(new Fields("followers"), new SpreadList(), new Fields("follower")) // get follower
				.groupBy(new Fields("follower")) // partition
				.aggregate(new One(), new Fields("one")) // Union
				.aggregate(new Fields("one"), new Sum(), new Fields("reach")); // Sum
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("socialnetwork", conf, buildTopology(drpc));
			
			LOG.info("Social network tuenti: " + drpc.execute("scope", "tuenti.com"));
			LOG.info("Social network twitter: " + drpc.execute("scope", "slashdot.org"));
			LOG.info("Social network googleplus: " + drpc.execute("scope", "googleplus"));
			//cluster.shutdown();
			//drpc.shutdown();
		} else {
			StormSubmitter.submitTopology("socialnetwork", conf, buildTopology(null));
			Thread.sleep(2000);
			DRPCClient drpcClient = new DRPCClient("vmlbcnimbusl01", 3772);
			
			LOG.info("Social network tuenti:: " + drpcClient.execute("scope", "tuenti.com"));
			LOG.info("Social network twitter: " + drpcClient.execute("scope", "slashdot.org"));
			LOG.info("Social network googleplus: " + drpcClient.execute("scope", "googleplus"));
		}
	}
}