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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducer;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.List;

public class CoGroupOperator extends Operator {

	CascadingTupleTypeInfo tupleType = new CascadingTupleTypeInfo();

	TypeInformation<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>> groupingSortingType =
			new TupleTypeInfo<Tuple3<CascadingTupleTypeInfo, Integer, CascadingTupleTypeInfo>>(
					tupleType, BasicTypeInfo.INT_TYPE_INFO, tupleType
			);

	private CoGroup coGroup;

	public CoGroupOperator(CoGroup coGroup, List<Operator> inputOps, FlowElementGraph flowGraph) {
		super(inputOps, coGroup, coGroup, flowGraph);

		this.coGroup = coGroup;
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputSets, List<Operator> inputOps) {

		if(inputOps.size() <= 1) {
			throw new RuntimeException("CoGroup requires at least two inputs");
		}

		boolean first = true;

		DataSet<Tuple3<Tuple, Integer, Tuple>> mergedSets = null;
		Scope[] incomingScopes = new Scope[inputOps.size()];

		for(int i=0; i<inputOps.size(); i++) {
			Operator inOp = inputOps.get(i);
			DataSet inSet = inputSets.get(i);

			Scope incomingScope = this.getIncomingScopeFrom(inOp);
			incomingScopes[i] = incomingScope;

			Fields groupByFields = coGroup.getKeySelectors().get(incomingScope.getName());
			Fields incomingFields = incomingScope.getOutGroupingFields();

			// build key Extractor mapper
			MapFunction keyExtractor = new CoGroupKeyExtractor(
					incomingFields,
					groupByFields,
					i);

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

		GroupReduceFunction coGroupReducer = new CoGroupReducer(coGroup, incomingScopes, getOutgoingScope());

		return mergedSets
				.groupBy(0)
				.sortGroup(1, Order.DESCENDING)
				.reduceGroup(coGroupReducer)
				.returns(tupleType)
				.name("CoGroup Joiner");


	}


}
