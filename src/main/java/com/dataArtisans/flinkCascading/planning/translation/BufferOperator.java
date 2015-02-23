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
import com.dataArtisans.flinkCascading.exec.operators.BufferReducer;
import com.dataArtisans.flinkCascading.exec.operators.KeyExtractor;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class BufferOperator extends Operator {

	private GroupBy groupBy;
	private Every buffer;

	private Scope bufferIncoming;
	private Scope bufferOutgoing;

	public BufferOperator(GroupBy groupBy, Every every, List<Scope> incomingScopes, Scope groupByOutgoingScope,
							Scope everyIncomingScope, Scope everyOutgoingScope, List<Operator> inputOps) {
		super(inputOps, incomingScopes, groupByOutgoingScope);

		if(!every.isBuffer()) {
			throw new RuntimeException("Every is not a buffer");
		}

		this.groupBy = groupBy;
		this.buffer = every;

		this.bufferIncoming = everyIncomingScope;
		this.bufferOutgoing = everyOutgoingScope;
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

		// build the group function
		GroupReduceFunction bufferReducer = new BufferReducer(this.buffer, this.bufferIncoming, this.bufferOutgoing);

				return inputs.get(0)
				.map(keyExtractor).name("KeyExtractor")
				.groupBy(0)
				.reduceGroup(bufferReducer).name(buffer.getName());

	}

	private List<Scope> getOutgoingScopes(List<Operator> inputOps) {
		List<Scope> scopes = new ArrayList<Scope>(inputOps.size());
		for(Operator op : inputOps) {
			scopes.add(op.getOutgoingScope());
		}
		return scopes;
	}

}
