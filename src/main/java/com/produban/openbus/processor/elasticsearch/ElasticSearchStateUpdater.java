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

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

/**
 * State Updater for Elastic Search
 * 
 */
public class ElasticSearchStateUpdater extends BaseStateUpdater<ElasticSearchState> {
	private static final long serialVersionUID = 8850462189864622257L;

	@Override
    public void updateState(ElasticSearchState eSstate, List<TridentTuple> tuples, TridentCollector collector) {
		eSstate.addDocuments(tuples);
    }
}