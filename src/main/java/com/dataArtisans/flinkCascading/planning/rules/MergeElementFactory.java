/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkCascading.planning.rules;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.tuple.Fields;

import java.util.Map;

public class MergeElementFactory implements ElementFactory {

	public static final String MERGE_FACTORY = "cascading.registry.merge";

	@Override
	public FlowElement create(ElementGraph graph, FlowElement flowElement) {

		GroupBy groupBy = (GroupBy)flowElement;
		Merge merge = new Merge(groupBy.getName(), groupBy.getPrevious());

		Map<String, Fields> keyMap = groupBy.getKeySelectors();
		// check that key fields are identical for all inputs
		Fields keyFields = null;
		for(Fields f : keyMap.values()) {
			if(keyFields == null) {
				keyFields = f;
			}
			else {
				if(!keyFields.equals(f)) {
					throw new RuntimeException("GroupBy on different fields not supported.");
				}
			}
		}

		Map<String, Fields> sortKeyMap = groupBy.getSortingSelectors();
		// check that key fields are identical for all inputs
		Fields sortFields = null;
		for(Fields f : sortKeyMap.values()) {
			if(sortFields == null) {
				sortFields = f;
			}
			else {
				if(!sortFields.equals(f)) {
					throw new RuntimeException("GroupBy on different fields not supported.");
				}
			}
		}

		keyMap.clear();
		keyMap.put(groupBy.getName(), keyFields);
		if(sortFields != null) {
			sortKeyMap.clear();
			sortKeyMap.put(groupBy.getName(), sortFields);
		}

		return merge;
	}
}
