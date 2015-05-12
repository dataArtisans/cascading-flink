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

package com.dataArtisans.flinkCascading.exec.operators;

import cascading.flow.planner.Scope;
import cascading.operation.Aggregator;
import cascading.operation.ConcreteCall;
import cascading.operation.GroupAssertion;
import cascading.pipe.Every;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import com.dataArtisans.flinkCascading.exec.FlinkCollector;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.exec.PassOnCollector;
import com.dataArtisans.flinkCascading.exec.TupleBuilderBuilder;
import com.dataArtisans.flinkCascading.exec.TupleBuilderCollector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@FunctionAnnotation.ForwardedFields("*")
public class GroupAssertionReducer extends RichGroupReduceFunction<Tuple3<Tuple,Tuple,Tuple>, Tuple3<Tuple,Tuple,Tuple>> {

	private Every[] everies;
	private Scope[] outgoingScopes;
	private Scope[] incomingScopes;
	private Fields groupingFields;

	private transient TupleEntry groupEntry;

	private transient GroupAssertion[] assertions;
	private transient TupleEntry[] argumentsEntries;
	private transient TupleBuilder[] argumentsBuilders;
	private transient ConcreteCall[] calls;
	private transient FlinkFlowProcess[] ffps;

	public GroupAssertionReducer(Every[] everies, Scope[] incomings, Scope[] outgoings, Fields groupingFields) {

		if(everies.length != outgoings.length) {
			throw new IllegalArgumentException("Number of everies and outgoing scopes must be equal.");
		}

		this.everies = everies;
		this.incomingScopes = incomings;
		this.outgoingScopes = outgoings;
		this.groupingFields = groupingFields;
	}

	@Override
	public void open(Configuration config) {

		int num = this.everies.length;

		this.groupEntry = new TupleEntry(groupingFields);

		// initialize assertions
		this.assertions = new GroupAssertion[num];
		this.argumentsEntries = new TupleEntry[num];
		this.argumentsBuilders = new TupleBuilder[num];
		this.calls = new ConcreteCall[num];
		this.ffps = new FlinkFlowProcess[num];
		for (int i=0; i<num; i++) {

			this.ffps[i] = new FlinkFlowProcess(this.getRuntimeContext());
			this.calls[i] = new ConcreteCall(outgoingScopes[i].getArgumentsDeclarator(), outgoingScopes[i].getOperationDeclaredFields());

			Fields argumentsSelector = outgoingScopes[i].getArgumentsSelector();

			argumentsEntries[i] = new TupleEntry(outgoingScopes[i].getArgumentsDeclarator(), true);
			argumentsBuilders[i] = TupleBuilderBuilder.createArgumentsBuilder(
					incomingScopes[i].getIncomingAggregatorArgumentFields(), argumentsSelector);

			calls[i].setArguments(argumentsEntries[i]);
			this.assertions[i] = this.everies[i].getGroupAssertion();
			this.assertions[i].prepare(ffps[i], calls[i]);
		}

	}

	@Override
	public void reduce(Iterable<Tuple3<Tuple, Tuple, Tuple>> vals, Collector<Tuple3<Tuple, Tuple, Tuple>> collector) throws Exception {

		boolean first = true;
		Tuple key;
		Tuple val;

		for(Tuple3<Tuple, Tuple, Tuple> v : vals) {

			key = v.f0;
			val = v.f2;

			// process assertions
			for(int i=0; i<this.assertions.length; i++) {

				// start group
				if (first) {
					// do only for first tuple of group
					groupEntry.setTuple(key);
					calls[i].setGroup(groupEntry); // set group key
					calls[i].setArguments(null);  // zero it out
					calls[i].setOutputCollector(null); // zero it out

					assertions[i].start(ffps[i], calls[i]);
				}

				argumentsEntries[i].setTuple(argumentsBuilders[i].makeResult(val, null));
				calls[i].setArguments(argumentsEntries[i]);
				assertions[i].aggregate(ffps[i], calls[i]);

				collector.collect(v);
			}
			first = false;
		}

		for(int i=0; i < this.assertions.length; i++) {
			this.assertions[i].doAssert(ffps[i], calls[i]);
		}
	}

}
