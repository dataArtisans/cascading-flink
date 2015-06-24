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

package com.dataArtisans.flinkCascading.exec;

import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.flow.stream.element.InputSource;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.pipe.joiner.BufferJoin;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;

import java.util.Iterator;

public class GroupByInGate extends GroupingSpliceGate implements InputSource {

	private FlinkGroupByClosure closure;

	private final boolean isBufferJoin;

	public GroupByInGate(FlowProcess flowProcess, Splice splice, IORole ioRole) {
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
			closure = new FlinkGroupByClosure(flowProcess, keyFields, valuesFields); // TODO what to do for CoGroupGates
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
		// receive not implemented for source groupBy
		throw new UnsupportedOperationException("Receive not implemented for GroupByInGate.");
	}

	@Override
	public void run(Object input) {

		if(!(input instanceof Iterator)) {
			throw new InvalidArgumentException("GroupByInGate requires Iterator<Tuple>");
		}

		KeyPeekingIterator keyPeekingIt = new KeyPeekingIterator((Iterator)input, keyBuilder[0]);

		closure.reset(keyPeekingIt);

		// Buffer is using JoinerClosure directly
		if( !isBufferJoin ) {
			tupleEntryIterator.reset(splice.getJoiner().getIterator(closure));
		}
		else {
			tupleEntryIterator.reset(keyPeekingIt);
		}

		Tuple groupTuple = keyPeekingIt.peekNextKey();
		keyEntry.setTuple( groupTuple );

		next.receive( this, grouping );

	}

	@Override
	public void complete( Duct previous )
	{
		if( next != null ) {
			super.complete(previous);
		}
	}

}
