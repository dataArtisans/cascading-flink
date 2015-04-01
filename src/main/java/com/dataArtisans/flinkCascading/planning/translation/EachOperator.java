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
import cascading.pipe.Each;
import com.dataArtisans.flinkCascading.exec.operators.EachFilter;
import com.dataArtisans.flinkCascading.exec.operators.EachFunctionMapper;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

public class EachOperator extends Operator {

	private Each each;

	public EachOperator(Each each, Operator inputOp, FlowElementGraph flowGraph) {
		super(inputOp, each, each, flowGraph);
		this.each = each;
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputSets, List<Operator> inputOps) {

		if(inputOps.size() != 1) {
			throw new IllegalArgumentException("Not exactly one input operator");
		}
		if(inputSets.size() != 1) {
			throw new IllegalArgumentException("Not exactly one input set");
		}

		Operator inputOp = inputOps.get(0);
		DataSet inputSet = inputSets.get(0);

		// get map function
		if(this.each.isFunction()) {
			return inputSet
					.flatMap(new EachFunctionMapper(each, getIncomingScopeFrom(inputOp), getOutgoingScope()))
					.returns(new CascadingTupleTypeInfo())
					.name(each.getName());
		}
		else if (this.each.isFilter()) {
			return inputSet
					.filter(new EachFilter(each, getIncomingScopeFrom(inputOp), getOutgoingScope()))
					.returns(new CascadingTupleTypeInfo())
					.name(each.getName());
		}
		else if (this.each.isValueAssertion()) {
			throw new UnsupportedOperationException("ValueAssertion not supported yet!");
		}
		else {
			throw new UnsupportedOperationException("Unsupported Each!");
		}

	}


}
