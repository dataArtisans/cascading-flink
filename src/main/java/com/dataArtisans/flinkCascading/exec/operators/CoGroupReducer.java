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
import cascading.flow.stream.duct.Grouping;
import cascading.pipe.CoGroup;
import cascading.pipe.joiner.BufferJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.util.TupleBuilder;
import com.dataArtisans.flinkCascading.exec.FlinkCoGroupClosure;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.exec.TupleBuilderBuilder;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class CoGroupReducer extends RichGroupReduceFunction<Tuple3<Tuple, Integer, Tuple>, Tuple> {

	private CoGroup coGroup;
	private Scope[] incomingScopes;
	private Scope outgoingScope;

	private int numInputs;

	private transient FlinkFlowProcess ffp;

	private transient Fields[] keyFields;
	private transient Fields[] valuesFields;

	private transient TupleBuilder[] keyBuilder;
	private transient TupleBuilder[] valuesBuilder;

	private transient Grouping<TupleEntry, TupleEntryIterator> grouping;
	private transient TupleEntry keyEntry;

//	private transient Collector collector;
	private transient FlinkCoGroupClosure closure;


	public CoGroupReducer(CoGroup coGroup, Scope[] incomings, Scope outgoing) {

		this.coGroup = coGroup;
		this.incomingScopes = incomings;
		this.outgoingScope = outgoing;
		this.numInputs = incomings.length;
	}

	@Override
	public void open(Configuration config) {

		this.ffp = new FlinkFlowProcess(this.getRuntimeContext());

		/// Duct.initialize

		keyFields = new Fields[ numInputs ];
		valuesFields = new Fields[ numInputs ];

		keyBuilder = new TupleBuilder[ numInputs ];
		valuesBuilder = new TupleBuilder[ numInputs ];

		for( int i = 0; i < numInputs; i++ )
		{
			Scope incomingScope = incomingScopes[i];

			keyFields[i] = outgoingScope.getKeySelectors().get( incomingScope.getName() );
			valuesFields[i] = incomingScope.getIncomingSpliceFields();

			keyBuilder[i] = TupleBuilderBuilder.createNarrowBuilder(incomingScope.getIncomingSpliceFields(), keyFields[i]);
			valuesBuilder[i] = TupleBuilderBuilder.createNulledBuilder(incomingScope.getIncomingSpliceFields(), keyFields[i]);

		}

		keyEntry = new TupleEntry( outgoingScope.getOutGroupingFields(), true );

		grouping = new Grouping();
		grouping.key = keyEntry;

		//// Duct.prepare()

		closure = new FlinkCoGroupClosure( ffp, coGroup.getNumSelfJoins(), keyFields, valuesFields );
		grouping.joinerClosure = closure;

	}

	@Override
	public void reduce(Iterable<Tuple3<Tuple, Integer, Tuple>> vals, Collector<Tuple> collector) throws Exception {

		closure.reset( vals.iterator() );

		Iterator<Tuple> joinedTuples = null;

		if( !( coGroup.getJoiner() instanceof BufferJoin) ) {
			joinedTuples = coGroup.getJoiner().getIterator(closure);
		}
		else {
			throw new RuntimeException("BufferJoin encountered.");
		}

		keyEntry.setTuple( closure.getGroupTuple( closure.getGrouping() ) );

		while(joinedTuples.hasNext()) {
			Tuple r = joinedTuples.next();
			collector.collect(r);
		}

	}

}
