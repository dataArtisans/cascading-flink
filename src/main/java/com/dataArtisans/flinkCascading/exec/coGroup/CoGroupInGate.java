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

package com.dataArtisans.flinkCascading.exec.coGroup;

import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.flow.stream.element.InputSource;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.CoGroup;
import cascading.pipe.joiner.BufferJoin;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import com.dataArtisans.flinkCascading.exec.coGroup.FlinkCoGroupClosure;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CoGroupInGate extends GroupingSpliceGate implements InputSource {

	private FlinkCoGroupClosure closure;

	private final boolean isBufferJoin;

	public CoGroupInGate(FlowProcess flowProcess, CoGroup splice, IORole ioRole) {
		super(flowProcess, splice, ioRole);

		this.isBufferJoin = splice.getJoiner() instanceof BufferJoin;
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
			//
			throw new UnsupportedOperationException("Non-source group by not supported in GroupByInGate");
		}

		if( role != IORole.sink ) {
			closure = new FlinkCoGroupClosure(flowProcess, this.getSplice().getNumSelfJoins(), keyFields, valuesFields); // TODO what to do for CoGroupGates
		}

		if( grouping != null && splice.getJoinDeclaredFields() != null && splice.getJoinDeclaredFields().isNone() ) {
			grouping.joinerClosure = closure;
		}
	}

	@Override
	public void start( Duct previous )
	{
		if( next != null ) {
			super.start(previous);
		}
	}

	public void receive( Duct previous, TupleEntry incomingEntry ) {
		// receive not implemented for source coGroup
		throw new UnsupportedOperationException("Receive not implemented for CoGroupInGate.");
	}

	@Override
	public void run(Object input) {

		if(!(input instanceof Iterator)) {
			throw new IllegalArgumentException("CoGroupInGate requires Iterator<Tuple>");
		}

		KeyPeekingIterator iterator = new KeyPeekingIterator((Iterator)input);
		Tuple key = iterator.peekNextKey();

		closure.reset(iterator);

		// Buffer is using JoinerClosure directly
		if( !isBufferJoin ) {
			tupleEntryIterator.reset(splice.getJoiner().getIterator(closure));
		}

		keyEntry.setTuple( this.closure.getGroupTuple(key) );

		next.receive( this, grouping );

	}

	@Override
	public void complete( Duct previous )
	{
		if( next != null ) {
			super.complete(previous);
		}
	}

	private static class KeyPeekingIterator implements Iterator<Tuple3<Tuple, Integer, Tuple>> {

		private final Iterator<Tuple3<Tuple, Integer, Tuple>> values;

		private Tuple3<Tuple, Integer, Tuple> peekedValue;
		private Tuple peekedKey;

		public KeyPeekingIterator(Iterator<Tuple3<Tuple, Integer, Tuple>> values) {
			this.values = values;
		}

		public Tuple peekNextKey() {
			if(peekedValue == null && values.hasNext()) {
				peekedValue = values.next();
				peekedKey = peekedValue.f0;
			}
			if(peekedKey != null) {
				return peekedKey;
			}
			else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public boolean hasNext() {
			return peekedValue != null || values.hasNext();
		}

		@Override
		public Tuple3<Tuple, Integer, Tuple> next() {

			if(peekedValue != null) {
				Tuple3<Tuple, Integer, Tuple> v = peekedValue;
				peekedValue = null;
				peekedKey = null;
				return v;
			}
			else {
				return values.next();
			}
		}
	}

}
