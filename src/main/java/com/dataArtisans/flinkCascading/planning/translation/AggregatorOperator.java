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
import java.util.List;
import java.util.Map;


public class AggregatorOperator extends Operator {

	private GroupBy groupBy;
	private List<Every> aggregators;

	private List<Scope> allIncoming;
	private List<Scope> allOutgoing;

	public AggregatorOperator(GroupBy groupBy, Every every, List<Scope> incomingScopes, Scope groupByOutgoingScope,
								Scope everyIncomingScope, Scope everyOutgoingScope, List<Operator> inputOps) {
		super(inputOps, incomingScopes, groupByOutgoingScope);

		this.groupBy = groupBy;
		this.aggregators = new ArrayList<Every>();

		allIncoming = new ArrayList<Scope>();
		allIncoming.add(null);

		allOutgoing = new ArrayList<Scope>();
		allOutgoing.add(groupByOutgoingScope);

		this.addAggregator(every, everyIncomingScope, everyOutgoingScope);

	}

	public void addAggregator(Every every, Scope incomingScope, Scope outgoingScope) {

		if(!every.isAggregator()) {
			throw new RuntimeException("Every is not an aggregator");
		}
		this.aggregators.add(every);

		// adapt and append outgoing scope
		outgoing = outgoingScope;
		allIncoming.add(incomingScope);
		allOutgoing.add(outgoingScope);

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

		// TODO: we need one key selector for each unioned input!
		String incomingName = getIncomingScope().getName();
		MapFunction keyExtractor = new KeyExtractor(
				getIncomingScope().getOutValuesFields(),
				groupBy.getKeySelectors().get("wc"),
				groupBy.getSortingSelectors().get("wc"));

		Every[] aggregatorsA = aggregators.toArray(new Every[aggregators.size()]);

		Scope[] inA = new Scope[aggregators.size()];
		for(int i=0; i<inA.length; i++) {
			inA[i] = allIncoming.get(i+1);
		}

		Scope[] outA = new Scope[aggregators.size()]; // these are the out scopes of all aggregators
		for(int i=0; i<outA.length; i++) {
			outA[i] = allOutgoing.get(i+1);
		}
		// build the group function
		GroupReduceFunction aggregationReducer = new AggregatorsReducer(aggregatorsA, inA, outA);
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
