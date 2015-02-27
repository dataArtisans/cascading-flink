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
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.AggregatorsReducer;
import com.dataArtisans.flinkCascading.exec.operators.GroupByKeyExtractor;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class AggregatorOperator extends Operator {

	private GroupBy groupBy;
	private List<Every> aggregators;

	public AggregatorOperator(GroupBy groupBy, Every every, List<Operator> inputOps, FlowElementGraph flowGraph) {

		super(inputOps, groupBy, every, flowGraph);

		this.groupBy = groupBy;
		this.aggregators = new ArrayList<Every>();

		this.addAggregator(every);

	}

	public void addAggregator(Every every) {

		if(!every.isAggregator()) {
			throw new RuntimeException("Every is not an aggregator");
		}
		this.aggregators.add(every);
		this.setOutgoingPipe(every);
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputSets, List<Operator> inputOps) {

		boolean first = true;
		boolean secondarySort = false;

		DataSet<Tuple3<Tuple, Tuple, Tuple>> mergedSets = null;

		for(int i=0; i<inputOps.size(); i++) {
			Operator inOp = inputOps.get(i);
			DataSet inSet = inputSets.get(i);

			Scope incomingScope = getIncomingScopeFrom(inOp);

			Fields groupByFields = groupBy.getKeySelectors().get(incomingScope.getName());
			Fields sortByFields = groupBy.getSortingSelectors().get(incomingScope.getName());
			Fields incomingFields = incomingScope.getOutGroupingFields();

			if(sortByFields != null) {
				secondarySort = true;
			}

			// build key Extractor mapper
			MapFunction keyExtractor = new GroupByKeyExtractor(
					incomingFields,
					groupByFields,
					sortByFields);

			if(first) {
				mergedSets = inSet.map(keyExtractor).name("Key Extractor");
				first = false;
			} else {
				mergedSets = mergedSets.union(inSet.map(keyExtractor).name("Key Extractor"));
			}
		}

		Every[] aggregatorsA = aggregators.toArray(new Every[aggregators.size()]);

		Scope[] inA = new Scope[aggregators.size()];
		inA[0] = this.getScopeBetween(groupBy, aggregatorsA[0]);
		for(int i=1; i<inA.length; i++) {
			inA[i] = this.getScopeBetween(aggregatorsA[i-1],aggregatorsA[i]);
		}

		Scope[] outA = new Scope[aggregators.size()]; // these are the out scopes of all aggregators
		for(int i=0; i<outA.length-1; i++) {
			outA[i] = this.getScopeBetween(aggregatorsA[i], aggregatorsA[i+1]);
		}
		outA[outA.length-1] = this.getOutgoingScope();

		// build the group function
		GroupReduceFunction aggregationReducer = new AggregatorsReducer(aggregatorsA, inA, outA);

		if(secondarySort) {
			return mergedSets
					.groupBy(0)
					.sortGroup(1, Order.ASCENDING)
					.reduceGroup(aggregationReducer).name("Aggregators");

		} else {

			return mergedSets
					.groupBy(0)
					.reduceGroup(aggregationReducer).name("Aggregators");
		}
	}

}
