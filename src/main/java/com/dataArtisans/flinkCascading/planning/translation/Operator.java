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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class Operator {

	private List<Operator> inputOps;
	private FlowElement lastCOp;
	private FlowElementGraph flowGraph;

	private DataSet memo = null;

	public Operator(Operator inputOp, FlowElement lastCOp, FlowElementGraph flowGraph) {

		this(Collections.singletonList(inputOp), lastCOp, flowGraph);
	}

	public Operator(List<Operator> inputOps, FlowElement lastCOp, FlowElementGraph flowGraph) {

		this.inputOps = inputOps;
		this.lastCOp = lastCOp;
		this.flowGraph = flowGraph;
	}

	protected void setLastCascadingOp(FlowElement lastCOp) {
		this.lastCOp = lastCOp;
	}

	public DataSet getFlinkOperator(ExecutionEnvironment env) {

		// check if already translated
		if(this.memo == null) {

			// get all inputs
			List<DataSet> inputs = new ArrayList<DataSet>();
			for (Operator inOp : inputOps) {
				DataSet input = inOp.getFlinkOperator(env);
				inputs.add(input);
			}

			// translate this operator
			this.memo = translateToFlink(env, inputs, inputOps);
		}

		return this.memo;
	}

	protected abstract DataSet translateToFlink(ExecutionEnvironment env,
												List<DataSet> inputs, List<Operator> inputOps);

	protected Scope getOutgoingScope() {
		return getOutgoingScopeFor(this.lastCOp);
	}

	protected Scope getOutgoingScopeFor(FlowElement e) {
		Set<Scope> outScopes = this.flowGraph.outgoingEdgesOf(e);
		if(outScopes.size() < 0) {
			throw new RuntimeException(this.lastCOp+" has no outgoing scope");
		}
		else if(outScopes.size() > 1) {
			throw new RuntimeException(this.lastCOp+" has more than one outgoing scope");
		}

		return outScopes.iterator().next();
	}

	protected Scope getIncomingScopeFor(FlowElement e) {
		Set<Scope> inScopes = this.flowGraph.incomingEdgesOf(e);
		if(inScopes.size() < 0) {
			throw new RuntimeException(this.lastCOp+" has no outgoing scope");
		}
		else if(inScopes.size() > 1) {
			throw new RuntimeException(this.lastCOp+" has more than one outgoing scope");
		}

		return inScopes.iterator().next();
	}

	protected Scope getAnyIncomingScopeFor(FlowElement e) {
		Set<Scope> inScopes = this.flowGraph.incomingEdgesOf(e);
		if(inScopes.size() < 0) {
			throw new RuntimeException(this.lastCOp+" has no outgoing scope");
		}

		return inScopes.iterator().next();
	}
}
