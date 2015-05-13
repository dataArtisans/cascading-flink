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
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.joiner.BufferJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.AggregatorsReducer;
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.GroupAssertionReducer;
import com.dataArtisans.flinkCascading.exec.operators.IdentityReducer;
import com.dataArtisans.flinkCascading.exec.operators.JoinKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.JoinReducer;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducerBufferJoin;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducerForEvery;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import com.dataArtisans.flinkCascading.planning.translation.GroupByOperator.EveryType;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CoGroupOperator extends Operator {

	CascadingTupleTypeInfo tupleType = new CascadingTupleTypeInfo();

	TypeInformation<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>> groupingAggregationType =
			new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>>(
					tupleType, tupleType, tupleType
			);

	private CoGroup coGroup;
	private List<Every> functions;
	private List<Every> assertions;

	private FlowElement lastAdded;
	private HashMap<FlowElement, Scope> incomingScopes;
	private HashMap<FlowElement, Scope> outgoingScopes;

	private EveryType type;

	public CoGroupOperator(CoGroup coGroup, List<Operator> inputOps, FlowElementGraph flowGraph) {
		super(inputOps, coGroup, coGroup, flowGraph);

		this.coGroup = coGroup;
		this.type = EveryType.NONE;
		this.functions = new ArrayList<Every>();
		this.assertions = new ArrayList<Every>();

		this.lastAdded = coGroup;
		this.incomingScopes = new HashMap<FlowElement, Scope>();
		this.outgoingScopes = new HashMap<FlowElement, Scope>();
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
										List<DataSet> inputSets, List<Operator> inputOps,
										Configuration config) {

		// add latest outgoing scope
		this.outgoingScopes.put(lastAdded, this.getOutgoingScope());

		boolean first = true;

		DataSet<Tuple3<Tuple, Integer, Tuple>> mergedSets = null;
		Scope[] incomingScopes = new Scope[inputOps.size()];

		for(int i=0; i<inputOps.size(); i++) {
			Operator inOp = inputOps.get(i);
			DataSet inSet = inputSets.get(i);

			Scope incomingScope = this.getIncomingScopeFrom(inOp);
			incomingScopes[i] = incomingScope;

			Fields groupByFields = coGroup.getKeySelectors().get(incomingScope.getName());
			Fields incomingFields = incomingScope.getIncomingTapFields(); // TODO: need to distinguish whether predecessor is splice or not

			// build key Extractor mapper
			JoinKeyExtractor keyExtractor = new JoinKeyExtractor(
					incomingFields,
					groupByFields,
					i);

			CascadingTupleTypeInfo keyTupleInfo;
			if(groupByFields.hasComparators()) {
				keyTupleInfo = new CascadingTupleTypeInfo(groupByFields.getComparators());
			}
			else {
				keyTupleInfo = tupleType;
			}

			TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, CascadingTupleTypeInfo, CascadingTupleTypeInfo>> groupingSortingType =
					new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, CascadingTupleTypeInfo, CascadingTupleTypeInfo>>(
							keyTupleInfo, BasicTypeInfo.INT_TYPE_INFO, tupleType
					);

			// TODO: self-joins + custom joins -> single reduce
			// TODO: n-ary inner joins -> cascade of binary join operators
			// TODO: n-ary outer joins -> cascade of binary co-group operators

			if(first) {
				mergedSets = inSet
						.map(keyExtractor)
						.returns(groupingSortingType)
						.name("CoGroup Key Extractor");
				first = false;
			} else {
				mergedSets = mergedSets.union(inSet
						.map(keyExtractor)
						.returns(groupingSortingType)
						.name("CoGroup Key Extractor"));
			}
		}

		// build assertion reducer
		GroupReduceFunction reduceAssertion = null;
		if(this.assertions.size() > 0) {

			Fields groupByFields = this.incomingScopes.get(assertions.get(0)).getOutGroupingFields();

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

		if(!(this.coGroup.getJoiner() instanceof BufferJoin)) {

			if (this.type == EveryType.NONE) {

				if(reduceAssertion == null) {
					// CoGroup without everies
					GroupReduceFunction coGroupReducer = new JoinReducer(coGroup.getJoiner(), coGroup.getNumSelfJoins(), incomingScopes, getOutgoingScope());

					return mergedSets
							.groupBy(0)
							.sortGroup(1, Order.DESCENDING)
							.reduceGroup(coGroupReducer)
							.withParameters(config)
							.returns(tupleType)
							.name("CoGrouper " + coGroup.getName());
				}
				else {
					// CoGroup only with assertion
					GroupReduceFunction coGroupReducer = new CoGroupReducerForEvery(coGroup, incomingScopes, outgoingScopes.get(coGroup));

					return mergedSets
							.groupBy(0)
							.sortGroup(1, Order.DESCENDING)
							.reduceGroup(coGroupReducer)
							.withParameters(config)
							.returns(groupingAggregationType)
							.withForwardedFields("f0")
							.name("CoGrouper " + coGroup.getName())
							.groupBy(0)
							.reduceGroup(reduceAssertion)
							.withParameters(config)
							.returns(groupingAggregationType)
							.groupBy(0)
							.reduceGroup(new IdentityReducer())
							.returns(tupleType);
				}
			}
			else {

				// CoGroup with everies
				GroupReduceFunction coGroupReducer = new CoGroupReducerForEvery(coGroup, incomingScopes, outgoingScopes.get(coGroup));

				DataSet<Tuple3<Tuple, Tuple, Tuple>> joinedSet = mergedSets
						.groupBy(0)
						.sortGroup(1, Order.DESCENDING)
						.reduceGroup(coGroupReducer)
						.withParameters(config)
						.returns(groupingAggregationType)
						.withForwardedFields("f0")
						.name("CoGrouper " + coGroup.getName());

				GroupReduceFunction reduceFunction = null;
				Fields groupByFields = this.incomingScopes.get(functions.get(0)).getOutGroupingFields();

				switch(this.type) {
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

				if(reduceAssertion == null) {

					return joinedSet
							.groupBy(0)
							.reduceGroup(reduceFunction)
							.withParameters(config)
							.returns(tupleType)
							.name("CoGroup Every " + coGroup.getName());

				}
				else {

					return joinedSet
							.groupBy(0)
							.reduceGroup(reduceAssertion)
							.withParameters(config)
							.returns(groupingAggregationType)
							.groupBy(0)
							.reduceGroup(reduceFunction)
							.withParameters(config)
							.returns(tupleType)
							.name("CoGroup Every " + coGroup.getName());
				}
			}
		}
		else {
			// Buffer Join
			if (this.type == EveryType.BUFFER) {

				if(reduceAssertion != null) {
					// TODO
					throw new UnsupportedOperationException("GroupAssertion after BufferJoin not supported yet");
				}

				GroupReduceFunction coGroupReducer = new CoGroupReducerBufferJoin(coGroup, functions.get(0), incomingScopes, this.outgoingScopes.get(coGroup), getOutgoingScope());

				return mergedSets
						.groupBy(0)
						.sortGroup(1, Order.DESCENDING)
						.reduceGroup(coGroupReducer)
						.withParameters(config)
						.returns(tupleType)
						.name("CoGrouper " + coGroup.getName());
			}
			else if (this.type == EveryType.NONE) {
					throw new UnsupportedOperationException("CoGroup with BufferJoin must be followed by Buffer.");
			}
			else if (this.type == EveryType.AGGREGATION) {
					throw new UnsupportedOperationException("CoGroup with BufferJoin must be followed by Buffer.");
			}
			else {
				throw new RuntimeException("Invalid Every type encountered");
			}

		}


	}


}
