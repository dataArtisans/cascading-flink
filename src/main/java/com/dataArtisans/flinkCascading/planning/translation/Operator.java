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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Operator {

	private List<Scope> incoming;
	private Scope outgoing;

	private List<Operator> inputOps;

	private DataSet memo = null;

	public Operator() {

	}

	public Operator(Operator inputOp) {
		this.inputOps = Collections.singletonList(inputOp);
	}

	public Operator(List<Operator> inputOps) {
		this.inputOps = inputOps;
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
			this.memo = translateToFlink(env, inputs);
		}

		return this.memo;
	}

	protected abstract DataSet translateToFlink(ExecutionEnvironment env, List<DataSet> inputs);

	public List<Operator> getInputOperators() {
		return this.inputOps;
	}

	public void setIncomingScope(Scope incomingScope) {
		this.incoming = Collections.singletonList(incomingScope);
	}

	public void setIncomingScopes(List<Scope> incomingScopes) {
		this.incoming = incomingScopes;
	}

	public Scope getIncomingScope() {
		if(this.incoming.size() != 1) {
			throw new RuntimeException("Operator does not have exactly one incoming scope");
		}
		return this.incoming.get(0);
	}

	public List<Scope> getIncomingScopes() {
		return this.incoming;
	}

	public void setOutgoingScope(Scope outgoingScope) {
		this.outgoing = outgoingScope;
	}

	public Scope getOutgoingScope() {
		return this.outgoing;
	}


}
