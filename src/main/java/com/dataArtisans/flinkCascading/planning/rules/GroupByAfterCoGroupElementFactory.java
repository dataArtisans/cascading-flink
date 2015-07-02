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
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;

public class GroupByAfterCoGroupElementFactory implements ElementFactory {

	public static final String GROUPBY_FACTORY = "cascading.registry.groupBy";

	@Override
	public FlowElement create(ElementGraph graph, FlowElement flowElement) {

		CoGroup coGroup = (CoGroup)flowElement;

		// get keys of coGroup

//		Pipe[] inputs = coGroup.getPrevious();
		Fields keys = new Fields("num", "num2");

//		Set<Scope> inScopes = graph.incomingEdgesOf(coGroup);
//		Scope outScope = coGroup.outgoingScopeFor(inScopes);
//		Fields keys = outScope.getOutGroupingFields();
//		for(int i=0; i<inputs.length; i++) {
//			keys = keys.append(coGroup.getKeySelectors().get(inputs[i].getName()));
//		}
		// TODO: what to do with sorting keys
//		Fields firstInputSortKeys = coGroup.getSortingSelectors().get(firstCoGroupInput.getName());
//		boolean reverse = coGroup.isSortReversed();

		GroupBy groupBy = new GroupBy(coGroup, keys);

		return groupBy;
	}
}
