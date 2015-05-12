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
import cascading.pipe.HashJoin;
import cascading.pipe.joiner.BufferJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.AggregatorsReducer;
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducerBufferJoin;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducerForEvery;
import com.dataArtisans.flinkCascading.exec.operators.JoinKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.JoinReducer;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class HashJoinOperator extends Operator {

	CascadingTupleTypeInfo tupleType = new CascadingTupleTypeInfo();

	TypeInformation<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>> groupingAggregationType =
			new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>>(
					tupleType, tupleType, tupleType
			);

	private HashJoin hashJoin;

	public HashJoinOperator(HashJoin hashJoin, List<Operator> inputOps, FlowElementGraph flowGraph) {
		super(inputOps, hashJoin, hashJoin, flowGraph);

		this.hashJoin = hashJoin;
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

			Fields groupByFields = hashJoin.getKeySelectors().get(incomingScope.getName());
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
						.name("Join Key Extractor");
				first = false;
			} else {
				mergedSets = mergedSets.union(inSet
						.map(keyExtractor)
						.returns(groupingSortingType)
						.name("Join Key Extractor"));
			}
		}

		if(!(this.hashJoin.getJoiner() instanceof BufferJoin)) {

			GroupReduceFunction coGroupReducer = new JoinReducer(hashJoin.getJoiner(), hashJoin.getNumSelfJoins(), incomingScopes, getOutgoingScope());

			return mergedSets
					.groupBy(0)
					.sortGroup(1, Order.DESCENDING)
					.reduceGroup(coGroupReducer)
					.returns(tupleType)
					.name("Joiner");
		}
		else {
			// Buffer Join
			throw new UnsupportedOperationException("HashJoin with BufferJoin not supported.");
		}

	}


}
