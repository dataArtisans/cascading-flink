/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkCascading.runtime.coGroup.bufferJoin;

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
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CoGroupBufferInGate extends GroupingSpliceGate implements InputSource {

	private CoGroupBufferClosure closure;

	private final boolean isBufferJoin;

	public CoGroupBufferInGate(FlowProcess flowProcess, CoGroup splice, IORole ioRole) {
		super(flowProcess, splice, ioRole);

		this.isBufferJoin = splice.getJoiner() instanceof BufferJoin;
	}

	@SuppressWarnings("unchecked")
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
			throw new UnsupportedOperationException("Non-source group by not supported in CoGroupBufferInGate");
		}

		if( role != IORole.sink ) {
			closure = new CoGroupBufferClosure(flowProcess, this.getSplice().getNumSelfJoins(), keyFields, valuesFields);
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
		throw new UnsupportedOperationException("Receive not implemented for CoGroupBufferInGate.");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run(Object input) {

		KeyPeekingIterator iterator;
		try {
			iterator = new KeyPeekingIterator((Iterator<Tuple3<Tuple, Integer, Tuple>>)input);
		}
		catch(ClassCastException cce) {
			throw new RuntimeException("CoGroupBufferInGate requires Iterator<Tuple3<Tuple, Integer, Tuple>", cce);
		}

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

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

}
