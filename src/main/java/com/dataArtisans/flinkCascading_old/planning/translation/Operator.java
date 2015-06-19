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

package com.dataArtisans.flinkCascading_old.planning.translation;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.FlowElementGraph;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class Operator {

	private final List<Operator> inputOps;
	private final FlowElement inPipe;
	private FlowElement outPipe;
	private final FlowElementGraph flowGraph;

	private DataSet memo = null;


	public Operator(Operator inputOp, FlowElement inPipe, FlowElement outPipe, FlowElementGraph flowGraph) {

		this(Collections.singletonList(inputOp), inPipe, outPipe, flowGraph);
	}

	public Operator(List<Operator> inputOps, FlowElement inPipe, FlowElement outPipe, FlowElementGraph flowGraph) {

		this.inputOps = inputOps;
		this.inPipe = inPipe;
		this.outPipe = outPipe;
		this.flowGraph = flowGraph;
	}

	protected void setOutgoingPipe(FlowElement outPipe) {
		this.outPipe = outPipe;
	}

	public DataSet getFlinkOperator(ExecutionEnvironment env, Configuration config) {

		// check if already translated
		if(this.memo == null) {

			// get all inputs
			List<DataSet> inputs = new ArrayList<DataSet>();
			for (Operator inOp : inputOps) {
				DataSet input = inOp.getFlinkOperator(env, config);
				inputs.add(input);
			}

			// translate this operator
			this.memo = translateToFlink(env, inputs, inputOps, config);
		}

		return this.memo;
	}

	protected abstract DataSet translateToFlink(ExecutionEnvironment env,
												List<DataSet> inputs, List<Operator> inputOps,
												Configuration config);

	protected Scope getIncomingScopeFrom(Operator op) {
		return this.flowGraph.getEdge(op.outPipe, this.inPipe);
	}

	protected Scope getOutgoingScopeTo(Operator op) {
		return this.flowGraph.getEdge(this.outPipe, op.inPipe);
	}

	protected Scope getScopeBetween(FlowElement source, FlowElement sink) {
		return this.flowGraph.getEdge(source, sink);
	}

	protected Scope getOutgoingScope() {
		Set<Scope> outScopes = this.flowGraph.outgoingEdgesOf(this.outPipe);
		if(outScopes.size() != 1) {
			// throw new RuntimeException("Not exactly one outgoing scope");
		}

		return outScopes.iterator().next();
	}
}
