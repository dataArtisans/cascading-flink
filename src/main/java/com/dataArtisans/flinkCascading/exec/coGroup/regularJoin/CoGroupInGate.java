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

package com.dataArtisans.flinkCascading.exec.coGroup.regularJoin;

import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.flow.stream.element.InputSource;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.CoGroup;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.dataArtisans.flinkCascading.exec.hashJoin.JoinClosure;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CoGroupInGate extends GroupingSpliceGate implements InputSource {

	private JoinClosure closure;
	private JoinResultIterator resultIterator;

	public CoGroupInGate(FlowProcess flowProcess, CoGroup splice, IORole ioRole) {
		super(flowProcess, splice, ioRole);
	}

	@Override
	public void bind( StreamGraph streamGraph )
	{
		if( role != IORole.sink ) {
			next = getNextFor(streamGraph);
		}

		if( role == IORole.sink ) {
			setOrdinalMap(streamGraph);
		}
	}


	@Override
	public void prepare()
	{
		if( role != IORole.source ) {
			throw new UnsupportedOperationException("Non-source group by not supported in GroupByInGate");
		}

		if( role != IORole.sink ) {

			Fields[] keyFields;
			Fields[] valuesFields;

			if(splice.isSelfJoin()) {
				keyFields = new Fields[splice.getNumSelfJoins() + 1];
				valuesFields = new Fields[splice.getNumSelfJoins() + 1];
				for(int i=0; i<keyFields.length; i++) {
					keyFields[i] = super.keyFields[0];
					valuesFields[i] = super.valuesFields[0];
				}
			}
			else {
				keyFields = super.keyFields;
				valuesFields = super.valuesFields;
			}

			closure = new JoinClosure(flowProcess, keyFields, valuesFields);
		}

		if( grouping != null && splice.getJoinDeclaredFields() != null && splice.getJoinDeclaredFields().isNone() ) {
			grouping.joinerClosure = closure;
		}

		this.resultIterator = new JoinResultIterator(closure, this.splice.getJoiner());
	}

	@Override
	public void start( Duct previous )
	{
		if( next != null ) {
			super.start(previous);
		}
	}

	public void receive( Duct previous, TupleEntry incomingEntry ) {
		throw new UnsupportedOperationException("Receive not implemented for CoGroupInGate.");
	}

	@Override
	public void run(Object input) {

		Iterator<Tuple2<Tuple, Tuple[]>> inputIterator = (Iterator<Tuple2<Tuple, Tuple[]>>)input;

		resultIterator.reset(inputIterator);
		resultIterator.hasNext(); // load first element into closure

		tupleEntryIterator.reset(resultIterator);
		keyEntry.setTuple( this.closure.getGroupTuple(null) );

		next.receive( this, grouping );

	}

	@Override
	public void complete( Duct previous )
	{
		if( next != null ) {
			super.complete(previous);
		}
	}

	private static class JoinResultIterator implements Iterator<Tuple> {

		private Iterator<Tuple2<Tuple, Tuple[]>> input;

		private JoinClosure closure;
		private Joiner joiner;

		private Iterator<Tuple> joinedTuples;

		public JoinResultIterator(JoinClosure closure, Joiner joiner) {
			this.closure = closure;
			this.joiner = joiner;
		}

		public void reset(Iterator<Tuple2<Tuple, Tuple[]>> input) {
			this.input = input;
		}

		@Override
		public boolean hasNext() {
			if(joinedTuples != null && joinedTuples.hasNext()) {
				return true;
			}
			else {
				do {
					if(input.hasNext()) {
							closure.reset(input.next());
							joinedTuples = joiner.getIterator(closure);
					}
					else {
						return false;
					}
				} while(!joinedTuples.hasNext());
				return true;
			}
		}

		@Override
		public Tuple next() {
			if(this.hasNext()) {
				Tuple t = joinedTuples.next();
				return t;
			}
			else {
				throw new NoSuchElementException("Iterator is empty.");
			}
		}
	}


}
