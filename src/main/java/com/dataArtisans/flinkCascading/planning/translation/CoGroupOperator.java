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
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.joiner.BufferJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.AggregatorsReducer;
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.JoinKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.JoinReducer;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducerBufferJoin;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducerForEvery;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class CoGroupOperator extends Operator {

	CascadingTupleTypeInfo tupleType = new CascadingTupleTypeInfo();

	TypeInformation<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>> groupingAggregationType =
			new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>>(
					tupleType, tupleType, tupleType
			);

	private CoGroup coGroup;
	private List<Every> everies;

	public CoGroupOperator(CoGroup coGroup, List<Operator> inputOps, FlowElementGraph flowGraph) {
		super(inputOps, coGroup, coGroup, flowGraph);

		this.coGroup = coGroup;
		this.everies = new ArrayList<Every>();
	}

	public void addEvery(Every every) {

		if(every.isGroupAssertion()) {
			throw new RuntimeException("GroupAssertion not supported yet.");
		}

		if(everies.size() > 0) {
			if(everies.get(0).isBuffer()) {
				throw new RuntimeException("CoGroup already closed by Buffer.");
			}
			else if(everies.get(0).isGroupAssertion()) {
				throw new RuntimeException("CoGroup already closed by GroupAssertion.");
			}
			else if(everies.get(0).isAggregator() && !every.isAggregator()) {
				throw new RuntimeException("Only Aggregator may be added to a CoGroup with Aggregators.");
			}
		}

		this.everies.add(every);
		this.setOutgoingPipe(every);
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputSets, List<Operator> inputOps) {

		boolean first = true;

		DataSet<Tuple3<Tuple, Integer, Tuple>> mergedSets = null;
		Scope[] incomingScopes = new Scope[inputOps.size()];

		for(int i=0; i<inputOps.size(); i++) {
			Operator inOp = inputOps.get(i);
			DataSet inSet = inputSets.get(i);

			Scope incomingScope = this.getIncomingScopeFrom(inOp);
			incomingScopes[i] = incomingScope;

			Fields groupByFields = coGroup.getKeySelectors().get(incomingScope.getName());
			Fields incomingFields = incomingScope.getOutValuesFields();

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

		if(!(this.coGroup.getJoiner() instanceof BufferJoin)) {

			if (everies.size() == 0) {

			GroupReduceFunction coGroupReducer = new JoinReducer(coGroup.getJoiner(), coGroup.getNumSelfJoins(), incomingScopes, getOutgoingScope());

			return mergedSets
					.groupBy(0)
					.sortGroup(1, Order.DESCENDING)
					.reduceGroup(coGroupReducer)
					.returns(tupleType)
					.name("CoGrouper " + coGroup.getName());
			}
			else {

				GroupReduceFunction coGroupReducer = new CoGroupReducerForEvery(coGroup, incomingScopes, getScopeBetween(coGroup, everies.get(0)));

				DataSet<Tuple3<Tuple, Tuple, Tuple>> joinedSet = mergedSets
						.groupBy(0)
						.sortGroup(1, Order.DESCENDING)
						.reduceGroup(coGroupReducer)
						.returns(groupingAggregationType)
						.withForwardedFields("f0")
						.name("CoGrouper " + coGroup.getName());

				GroupReduceFunction reduceFunction = null;
				Fields groupByFields = getScopeBetween(coGroup, everies.get(0)).getOutGroupingFields();

				if (everies.get(0).isAggregator()) {

					Every[] aggregatorsA = everies.toArray(new Every[everies.size()]);

					Scope[] inA = new Scope[everies.size()];
					inA[0] = this.getScopeBetween(coGroup, aggregatorsA[0]);
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
				} else if (everies.get(0).isBuffer()) {
					Every buffer = everies.get(0);

					reduceFunction = new BufferReducer(buffer,
							this.getScopeBetween(coGroup, buffer), this.getOutgoingScope(), groupByFields);
				}

				return joinedSet
						.groupBy(0)
						.reduceGroup(reduceFunction)
						.returns(tupleType)
						.name("CoGroup Every " + coGroup.getName());
			}
		}
		else {
			// Buffer Join
			if (everies.size() == 1 && everies.get(0).isBuffer()) {

				GroupReduceFunction coGroupReducer = new CoGroupReducerBufferJoin(coGroup, everies.get(0), incomingScopes,
						getScopeBetween(coGroup, everies.get(0)), getOutgoingScope());

				return mergedSets
						.groupBy(0)
						.sortGroup(1, Order.DESCENDING)
						.reduceGroup(coGroupReducer)
						.returns(tupleType)
						.name("CoGrouper " + coGroup.getName());
			}
			else {

				if (everies.size() == 0) {
					throw new UnsupportedOperationException("CoGroup with BufferJoin must be followed by Buffer.");
				}
				else if (everies.size() == 1 && !everies.get(0).isBuffer()) {
					throw new UnsupportedOperationException("CoGroup with BufferJoin must be followed by Buffer.");
				}
				else {
					throw new UnsupportedOperationException("CoGroup with BufferJoin must be followed by a single Buffer and no other Every.");
				}

			}
		}


	}


}
