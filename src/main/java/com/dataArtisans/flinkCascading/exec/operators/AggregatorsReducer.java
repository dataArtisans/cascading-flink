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
import cascading.pipe.Every;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import com.dataArtisans.flinkCascading.exec.FlinkCollector;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.exec.PassOnCollector;
import com.dataArtisans.flinkCascading.exec.TupleBuilderCollector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AggregatorsReducer extends RichGroupReduceFunction<Tuple3<Tuple,Tuple,Tuple>, Tuple> {

	private Every[] everies;
	private Scope[] outgoingScopes;
	private Scope[] incomingScopes;

	private transient Aggregator[] aggregators;
	private transient TupleEntry[] argumentsEntries;
	private transient TupleBuilder[] argumentsBuilders;
	private transient TupleBuilder[] outgoingBuilders;
	private transient ConcreteCall[] calls;
	private transient FlinkFlowProcess[] ffps;


	public AggregatorsReducer(Every[] everies, Scope[] incomings, Scope[] outgoings) {

		if(everies.length != outgoings.length) {
			throw new IllegalArgumentException("Number of everies and outgoing scopes must be equal.");
		}

		this.everies = everies;
		this.incomingScopes = incomings;
		this.outgoingScopes = outgoings;
	}

	@Override
	public void open(Configuration config) {

		int num = everies.length;

		this.aggregators = new Aggregator[num];
		this.argumentsEntries = new TupleEntry[num];
		this.argumentsBuilders = new TupleBuilder[num];
		this.outgoingBuilders = new TupleBuilder[num];
		this.calls = new ConcreteCall[num];
		this.ffps = new FlinkFlowProcess[num];

		for (int i=0; i<everies.length; i++) {

			this.ffps[i] = new FlinkFlowProcess(this.getRuntimeContext());
			this.aggregators[i] = this.everies[i].getAggregator();

			this.calls[i] = new ConcreteCall(outgoingScopes[i].getArgumentsDeclarator(), outgoingScopes[i].getOperationDeclaredFields());

			Fields argumentsSelector = outgoingScopes[i].getArgumentsSelector();
			Fields remainderFields = outgoingScopes[i].getRemainderPassThroughFields();
			Fields outgoingSelector = outgoingScopes[i].getOutGroupingSelector();

			argumentsEntries[i] = new TupleEntry(outgoingScopes[i].getArgumentsDeclarator(), true);
			argumentsBuilders[i] = TupleBuilderBuilder.createArgumentsBuilder(
					incomingScopes[i].getIncomingAggregatorArgumentFields(), argumentsSelector);
			outgoingBuilders[i] = TupleBuilderBuilder.createOutgoingBuilder(
					everies[i], incomingScopes[i].getIncomingAggregatorPassThroughFields(), argumentsSelector,
					remainderFields, outgoingScopes[i].getOperationDeclaredFields(), outgoingSelector);

			calls[i].setArguments(argumentsEntries[i]);

			aggregators[i].prepare(ffps[i], calls[i]);
		}

	}

	@Override
	public void reduce(Iterable<Tuple3<Tuple, Tuple, Tuple>> vals, Collector<Tuple> collector) throws Exception {

		boolean first = true;
		Tuple key = null;
		Tuple val;

		for(Tuple3<Tuple, Tuple, Tuple> v : vals) {

			key = v.f0;
			val = v.f2;

			for(int i=0; i<aggregators.length; i++) {

				if (first) {
					// start group

					calls[i].setGroup(new TupleEntry(key)); // set group key
					calls[i].setArguments(null);  // zero it out
					calls[i].setOutputCollector(null); // zero it out

					// start group
					aggregators[i].start(ffps[i], calls[i]);

				}

				argumentsEntries[i].setTuple(argumentsBuilders[i].makeResult(val, null));
				calls[i].setArguments(argumentsEntries[i]);
				aggregators[i].aggregate(ffps[i], calls[i]);
			}
			first = false;

		}

		int i = this.aggregators.length-1;

		TupleBuilderCollector x = new FlinkCollector(collector, this.outgoingBuilders[i], outgoingScopes[i].getOperationDeclaredFields() );
		calls[i].setOutputCollector(x);
		calls[i].setArguments(null);
		for(i=this.aggregators.length-1; i > 0; i--) {
			// chain collectors
			x = new PassOnCollector(aggregators[i], x, ffps[i], calls[i], outgoingBuilders[i-1], outgoingScopes[i-1].getOperationDeclaredFields() );
			calls[i-1].setOutputCollector(x);
			calls[i-1].setArguments(null);
		}

		// finish group
		x.setInTuple(key);
		aggregators[0].complete(ffps[0], calls[0]);

	}

}
