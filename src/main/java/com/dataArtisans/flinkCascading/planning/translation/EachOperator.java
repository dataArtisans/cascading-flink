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

import cascading.pipe.Each;
import com.dataArtisans.flinkCascading.exec.operators.EachFunctionMapper;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Collections;
import java.util.List;


public class EachOperator extends Operator {

	private Each each;

	public EachOperator(Each each, Operator inputOp) {
		super(inputOp);
		this.each = each;

		// set scopes
		setIncomingScope(inputOp.getOutgoingScope());
		setOutgoingScope(each.outgoingScopeFor(Collections.singleton(getIncomingScope())));

	}

	protected DataSet translateToFlink(ExecutionEnvironment env, List<DataSet> inputs) {

		// get map function
		MapPartitionFunction mapper;
		if(this.each.isFunction()) {
			mapper = new EachFunctionMapper(each, getIncomingScope(), getOutgoingScope());
		}
		else if (this.each.isFilter()) {
			throw new UnsupportedOperationException("Filter not supported yet!");
		}
		else if (this.each.isValueAssertion()) {
			throw new UnsupportedOperationException("ValueAssertion not supported yet!");
		}
		else {
			throw new UnsupportedOperationException("Unsupported Each!");
		}

		return inputs.get(0)
				.mapPartition(mapper)
				.name(each.getName());

	}


}
