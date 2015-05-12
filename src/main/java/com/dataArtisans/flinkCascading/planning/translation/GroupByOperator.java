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

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.AggregatorsReducer;
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.GroupAssertionReducer;
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
import java.util.HashMap;
import java.util.List;

public class GroupByOperator extends Operator {

	public static enum EveryType {
		NONE,
		AGGREGATION,
		BUFFER
	}

	CascadingTupleTypeInfo tupleType = new CascadingTupleTypeInfo();

	private GroupBy groupBy;
	private List<Every> functions;
	private List<Every> assertions;

	private FlowElement lastAdded;
	private HashMap<FlowElement, Scope> incomingScopes;
	private HashMap<FlowElement, Scope> outgoingScopes;

	private EveryType type;

	public GroupByOperator(GroupBy groupBy, List<Operator> inputOps, FlowElementGraph flowGraph) {

		super(inputOps, groupBy, groupBy, flowGraph);

		this.groupBy = groupBy;
		this.type = EveryType.NONE;
		this.functions = new ArrayList<Every>();
		this.assertions = new ArrayList<Every>();

		this.incomingScopes = new HashMap<FlowElement, Scope>();
		this.outgoingScopes = new HashMap<FlowElement, Scope>();
		this.lastAdded = groupBy;

	}

	public void addEvery(Every every) {

		if(every.isGroupAssertion()) {
			switch(this.type) {
				case NONE:
					break;
			}
			this.assertions.add(every);
		}
		else if(every.isBuffer()) {
			switch(this.type) {
				case NONE:
					this.type = EveryType.BUFFER;
					this.functions.add(every);
					break;
				case BUFFER:
					throw new RuntimeException("Only one Buffer allowed after a GroupBy.");
				case AGGREGATION:
					throw new RuntimeException("A Buffer may not be added to a GroupBy with Aggregators.");
			}
		}
		else if(every.isAggregator()) {
			switch(this.type) {
				case NONE:
				case AGGREGATION:
					this.type = EveryType.AGGREGATION;
					this.functions.add(every);
					break;
				case BUFFER:
					throw new RuntimeException("GroupBy already closed by Buffer.");
			}
		}

		incomingScopes.put(every, this.getScopeBetween(lastAdded, every));
		outgoingScopes.put(lastAdded, this.getScopeBetween(lastAdded, every));

		this.setOutgoingPipe(every);
		this.lastAdded = every;
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
									   List<DataSet> inputSets, List<Operator> inputOps) {

		// add latest outgoing scope
		this.outgoingScopes.put(lastAdded, this.getOutgoingScope());

		boolean first = true;
		boolean secondarySort = false;

		DataSet<Tuple3<Tuple, Tuple, Tuple>> mergedSets = null;
		Fields groupByFields = null;
		TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, CascadingTupleTypeInfo, CascadingTupleTypeInfo>> groupingSortingType = null;

		// get unioned input for group-by
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

			groupingSortingType = new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, CascadingTupleTypeInfo, CascadingTupleTypeInfo>>(
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

		// build assertion reducer
		GroupReduceFunction reduceAssertion = null;
		if(this.assertions.size() > 0) {
			Every[] assertionsA = new Every[assertions.size()];
			for(int i=0;i<this.assertions.size(); i++) {
				assertionsA[i] = this.assertions.get(i);
			}

			Scope[] inA = new Scope[assertions.size()];
			for (int i = 0; i < inA.length; i++) {
				inA[i] = this.incomingScopes.get(assertionsA[i]);
			}

			Scope[] outA = new Scope[assertions.size()];
			for (int i = 0; i < outA.length; i++) {
					outA[i] = this.outgoingScopes.get(assertionsA[i]);
			}

			// build the group function
			reduceAssertion = new GroupAssertionReducer(assertionsA, inA, outA, groupByFields);
		}

		// build function reducer
		GroupReduceFunction reduceFunction = null;
		switch(this.type) {
			case NONE:
				// use identity reducer
				reduceFunction = new IdentityReducer();
				break;
			case AGGREGATION:

				Every[] aggregatorsA = functions.toArray(new Every[functions.size()]);

				Scope[] inA = new Scope[functions.size()];
				for (int i = 0; i < inA.length; i++) {
					inA[i] = this.incomingScopes.get(aggregatorsA[i]);
				}

				Scope[] outA = new Scope[functions.size()];
				for (int i = 0; i < outA.length; i++) {
					outA[i] = this.outgoingScopes.get(aggregatorsA[i]);
				}

				// build the group function
				reduceFunction = new AggregatorsReducer(aggregatorsA, inA, outA, groupByFields);
				break;
			case BUFFER:
				Every buffer = functions.get(0);

				reduceFunction = new BufferReducer(buffer,
						this.incomingScopes.get(buffer), this.outgoingScopes.get(buffer), groupByFields);
				break;
		}

		if(secondarySort) {

			if(reduceAssertion == null) {
				return mergedSets
						.groupBy(0)
						.sortGroup(1, Order.ASCENDING)
						.reduceGroup(reduceFunction)
						.returns(tupleType)
						.name("GroupBy " + groupBy.getName());
			}
			else {
				return mergedSets
						.groupBy(0)
						.sortGroup(1, Order.ASCENDING)
						.reduceGroup(reduceAssertion)
						.returns(groupingSortingType)
						.groupBy(0)
						.sortGroup(1, Order.ASCENDING)
						.reduceGroup(reduceFunction)
						.returns(tupleType)
						.name("GroupBy " + groupBy.getName());
			}

		} else {

			if(reduceAssertion == null) {
				return mergedSets
						.groupBy(0)
						.reduceGroup(reduceFunction)
						.returns(tupleType)
						.name("GroupBy " + groupBy.getName());
			}
			else {
				return mergedSets
						.groupBy(0)
						.reduceGroup(reduceAssertion)
						.returns(groupingSortingType)
						.groupBy(0)
						.reduceGroup(reduceFunction)
						.returns(tupleType)
						.name("GroupBy " + groupBy.getName());
			}
		}
	}

}
