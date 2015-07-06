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

package com.dataArtisans.flinkCascading_old.exec.operators;

import cascading.flow.planner.Scope;
import cascading.operation.Buffer;
import cascading.operation.ConcreteCall;
import cascading.pipe.Every;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import com.dataArtisans.flinkCascading_old.exec.FlinkArgumentsIterator;
import com.dataArtisans.flinkCascading_old.exec.FlinkCollector;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import com.dataArtisans.flinkCascading_old.exec.TupleBuilderBuilder;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class BufferReducer extends RichGroupReduceFunction<Tuple3<Tuple,Tuple,Tuple>, Tuple> {

	private Every every;
	private Scope outgoingScope;
	private Scope incomingScope;
	private Fields groupingFields;

	private transient Buffer buffer;
	private transient TupleEntry groupEntry;
	private transient TupleEntry argumentsEntry;
	private transient TupleEntry passThroughEntry;
	private transient TupleBuilder argumentsBuilder;
	private transient TupleBuilder outgoingBuilder;
	private transient ConcreteCall call;
	private transient FlinkFlowProcess ffp;


	public BufferReducer(Every every, Scope incoming, Scope outgoing, Fields groupingFields) {

		this.every = every;
		this.incomingScope = incoming;
		this.outgoingScope = outgoing;
		this.groupingFields = groupingFields;
	}

	@Override
	public void open(Configuration config) {

		this.ffp = new FlinkFlowProcess(new org.apache.hadoop.conf.Configuration(), this.getRuntimeContext());
		this.buffer = this.every.getBuffer();

		this.call = new ConcreteCall(outgoingScope.getArgumentsDeclarator(), outgoingScope.getOperationDeclaredFields());

		this.groupEntry = new TupleEntry(this.groupingFields);

		Fields argumentsSelector = outgoingScope.getArgumentsSelector();
		Fields remainderFields = outgoingScope.getRemainderPassThroughFields();
		Fields outgoingSelector = outgoingScope.getOutGroupingSelector();

		argumentsEntry = new TupleEntry(outgoingScope.getArgumentsDeclarator(), true);
		argumentsBuilder = TupleBuilderBuilder.createArgumentsBuilder(
				incomingScope.getIncomingBufferArgumentFields(), argumentsSelector);

		Fields passThroughFields = outgoingScope.getIncomingAggregatorPassThroughFields();
		passThroughEntry = new TupleEntry(passThroughFields, false);
		passThroughEntry.setTuple(new Tuple(new Object[passThroughFields.size()]));

		outgoingBuilder = TupleBuilderBuilder.createOutgoingBuilder(
				every, incomingScope.getIncomingBufferPassThroughFields(), argumentsSelector,
				remainderFields, outgoingScope.getOperationDeclaredFields(), outgoingSelector);

		call.setArguments(argumentsEntry);

		buffer.prepare(ffp, call);

	}

	@Override
	public void reduce(Iterable<Tuple3<Tuple, Tuple, Tuple>> vals, Collector<Tuple> collector) throws Exception {

		FlinkCollector flinkCollector = new FlinkCollector(collector, outgoingBuilder, outgoingScope.getOperationDeclaredFields());
		FlinkArgumentsIterator argIt = new FlinkArgumentsIterator(vals, this.argumentsEntry, this.argumentsBuilder);
		argIt.setTupleBuildingCollector(flinkCollector);

		call.setArgumentsIterator( argIt );

		// init pass-through values with grouping key
		if(passThroughEntry.getFields().contains(this.groupingFields)) {
			// TODO: Check what happens if only a subset of the grouping keys is in the pass-through fields
			passThroughEntry.setTuple(this.groupingFields, argIt.getKey());
		}
		flinkCollector.setInTuple(passThroughEntry.getTuple());

		this.groupEntry.setTuple(argIt.getKey());
		call.setGroup( this.groupEntry );
		call.setOutputCollector( flinkCollector );

		buffer.operate( ffp, call );

	}


}
