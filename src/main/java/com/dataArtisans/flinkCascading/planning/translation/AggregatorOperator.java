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

package com.dataArtisans.flinkCascading.planning.translation;

import cascading.flow.planner.Scope;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.exec.operators.AggregatorsReducer;
import com.dataArtisans.flinkCascading.exec.operators.KeyExtractor;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class AggregatorOperator extends Operator {

	private GroupBy groupBy;
	private List<Every> aggregators;

	private List<Scope> allOutgoing;

	public AggregatorOperator(GroupBy groupBy, Every every, List<Operator> inputOps) {
		super(inputOps);

		this.groupBy = groupBy;
		this.aggregators = new ArrayList<Every>();

		setIncomingScopes(getOutgoingScopes(inputOps));
		setOutgoingScope(groupBy.outgoingScopeFor(new HashSet<Scope>(getIncomingScopes())));

		allOutgoing = new ArrayList<Scope>();
		allOutgoing.add(getOutgoingScope());

		this.addAggregator(every);
	}

	public void addAggregator(Every every) {

		if(!every.isAggregator()) {
			throw new RuntimeException("Every is not an aggregator");
		}
		this.aggregators.add(every);

		// adapt and append outgoing scope
		setOutgoingScope(every.outgoingScopeFor(Collections.singleton(getOutgoingScope())));
		allOutgoing.add(getOutgoingScope());
	}

	protected DataSet translateToFlink(ExecutionEnvironment env, List<DataSet> inputs) {

		// TODO: handle union

		// build key extractor
		Map<String, Fields> groupingKeys = groupBy.getKeySelectors();
		Map<String, Fields> sortingKeys = groupBy.getSortingSelectors();

		// TODO: remove this limitation
		if(sortingKeys.size() > 0) {
			throw new RuntimeException("Secondary Sort not yet supported");
		}

		// TODO: remove static string
		String incomingName = getIncomingScope().getName();
		MapFunction keyExtractor = new KeyExtractor(
				getIncomingScope().getOutValuesFields(),  // TODO getOutGroupingValueFields give value fields without grouping fields
				getIncomingScope().getOutGroupingFields(),
				null);


		// TODO: remove restriction for single aggregator
		if(aggregators.size() != 1) {
			throw new RuntimeException("Currently only one aggregator supported");
		}

		// build the group function
		GroupReduceFunction aggregationReducer = new AggregatorsReducer(aggregators.get(0), allOutgoing.get(0), allOutgoing.get(1));
//		throw new RuntimeException("Implement AggregatorsReducer");
//
		return inputs.get(0)
				.map(keyExtractor).name("KeyExtractor")
				.groupBy(0)
				.reduceGroup(aggregationReducer).name("Aggregators"); // TODO set name

	}

	private List<Scope> getOutgoingScopes(List<Operator> inputOps) {
		List<Scope> scopes = new ArrayList<Scope>(inputOps.size());
		for(Operator op : inputOps) {
			scopes.add(op.getOutgoingScope());
		}
		return scopes;
	}

}
