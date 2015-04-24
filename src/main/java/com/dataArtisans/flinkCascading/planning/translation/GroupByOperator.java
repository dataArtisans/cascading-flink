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
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.GroupByKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.IdentityReducer;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupByOperator extends Operator {

	CascadingTupleTypeInfo tupleType = new CascadingTupleTypeInfo();

	private GroupBy groupBy;
	private List<Every> everies;

	public GroupByOperator(GroupBy groupBy, List<Operator> inputOps, FlowElementGraph flowGraph) {

		super(inputOps, groupBy, groupBy, flowGraph);

		this.groupBy = groupBy;
		this.everies = new ArrayList<Every>();

	}

	public void addEvery(Every every) {

		if(every.isGroupAssertion()) {
			throw new RuntimeException("GroupAssertion not supported yet.");
		}

		if(everies.size() > 0) {
			if(everies.get(0).isBuffer()) {
				throw new RuntimeException("GroupBy already closed by Buffer.");
			}
			else if(everies.get(0).isGroupAssertion()) {
				throw new RuntimeException("GroupBy already closed by GroupAssertion.");
			}
			else if(everies.get(0).isAggregator() && !every.isAggregator()) {
				throw new RuntimeException("Only Aggregator may be added to a GroupBy with Aggregators.");
			}
		}

		this.everies.add(every);
		this.setOutgoingPipe(every);
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputSets, List<Operator> inputOps) {

		boolean first = true;
		boolean secondarySort = false;

		DataSet<Tuple3<Tuple, Tuple, Tuple>> mergedSets = null;
		Fields groupByFields = null;

		for(int i=0; i<inputOps.size(); i++) {
			Operator inOp = inputOps.get(i);
			DataSet inSet = inputSets.get(i);

			Scope incomingScope = getIncomingScopeFrom(inOp);

			groupByFields = groupBy.getKeySelectors().get(incomingScope.getName());
			Fields sortByFields = groupBy.getSortingSelectors().get(incomingScope.getName());
			Fields incomingFields = groupBy.outgoingScopeFor(Collections.singleton(incomingScope)).getOutValuesFields();

			if(sortByFields != null) {
				secondarySort = true;
			}

			CascadingTupleTypeInfo keyTupleInfo;
			if(groupByFields.hasComparators()) {
				keyTupleInfo = new CascadingTupleTypeInfo(groupByFields.getComparators());
			}
			else {
				keyTupleInfo = tupleType;
			}

			TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, CascadingTupleTypeInfo, CascadingTupleTypeInfo>> groupingSortingType =
				new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, CascadingTupleTypeInfo, CascadingTupleTypeInfo>>(
						keyTupleInfo, tupleType, tupleType
				);

			// build key Extractor mapper
			MapFunction keyExtractor = new GroupByKeyExtractor(
					incomingFields,
					groupByFields,
					sortByFields);

			if(first) {
				mergedSets = inSet.map(keyExtractor)
						.returns(groupingSortingType)
						.name("Key Extractor");
				first = false;
			} else {
				mergedSets = mergedSets.union(inSet
						.map(keyExtractor)
						.returns(groupingSortingType)
						.name("Key Extractor"));
			}
		}

		GroupReduceFunction reduceFunction = null;

		if(everies.size() == 0) {
			// use identity reducer
			reduceFunction = new IdentityReducer();
		}
		else if(everies.get(0).isAggregator()) {

			Every[] aggregatorsA = everies.toArray(new Every[everies.size()]);

			Scope[] inA = new Scope[everies.size()];
			inA[0] = this.getScopeBetween(groupBy, aggregatorsA[0]);
			for (int i = 1; i < inA.length; i++) {
				inA[i] = this.getScopeBetween(aggregatorsA[i - 1], aggregatorsA[i]);
			}

			Scope[] outA = new Scope[everies.size()]; // these are the out scopes of all aggregators
			for (int i = 0; i < outA.length - 1; i++) {
				outA[i] = this.getScopeBetween(aggregatorsA[i], aggregatorsA[i + 1]);
			}
			outA[outA.length - 1] = this.getOutgoingScope();

			// build the group function
			reduceFunction = new AggregatorsReducer(aggregatorsA, inA, outA, groupByFields);
		}
		else if(everies.get(0).isBuffer()) {
			Every buffer = everies.get(0);

			reduceFunction = new BufferReducer(buffer,
							this.getScopeBetween(groupBy, buffer), this.getOutgoingScope(), groupByFields);
		}

		if(secondarySort) {
			return mergedSets
					.groupBy(0)
					.sortGroup(1, Order.ASCENDING)
					.reduceGroup(reduceFunction)
					.returns(tupleType)
					.name("GroupBy "+groupBy.getName());

		} else {

			return mergedSets
					.groupBy(0)
					.reduceGroup(reduceFunction)
					.returns(tupleType)
					.name("GroupBy "+groupBy.getName());
		}
	}

}
