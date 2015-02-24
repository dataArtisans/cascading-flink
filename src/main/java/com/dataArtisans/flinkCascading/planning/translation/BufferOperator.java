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

import cascading.flow.planner.graph.FlowElementGraph;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.KeyExtractor;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;


public class BufferOperator extends Operator {

	private GroupBy groupBy;
	private Every buffer;

	public BufferOperator(GroupBy groupBy, Every every, List<Operator> inputOps, FlowElementGraph flowGraph) {
		super(inputOps, every, flowGraph);

		if(!every.isBuffer()) {
			throw new RuntimeException("Every is not a buffer");
		}

		this.groupBy = groupBy;
		this.buffer = every;

	}

	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputSets, List<Operator> inputOps) {

		boolean first = true;
		boolean secondarySort = false;

		DataSet<Tuple3<Tuple, Tuple, Tuple>> mergedSets = null;

		for(int i=0; i<inputOps.size(); i++) {
			Operator inOp = inputOps.get(i);
			DataSet inSet = inputSets.get(i);

			Fields groupByFields = groupBy.getKeySelectors().get(inOp.getOutgoingScope().getName());
			Fields sortByFields = groupBy.getSortingSelectors().get(inOp.getOutgoingScope().getName());
			Fields incomingFields = getAnyIncomingScopeFor(groupBy).getOutGroupingFields();

			if(sortByFields != null) {
				secondarySort = true;
			}

			// build key Extractor mapper
			MapFunction keyExtractor = new KeyExtractor(
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

		// build the group function
		GroupReduceFunction bufferReducer =
				new BufferReducer(this.buffer,
						this.getIncomingScopeFor(buffer), this.getOutgoingScopeFor(buffer));

		if(secondarySort) {
			return 	mergedSets
					.groupBy(0).sortGroup(1, Order.ASCENDING)
					.reduceGroup(bufferReducer).name(buffer.getName());
		} else {
			return mergedSets
					.groupBy(0)
					.reduceGroup(bufferReducer).name(buffer.getName());
		}
	}

}
