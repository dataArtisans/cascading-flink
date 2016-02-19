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

package com.dataartisans.flink.cascading.runtime.hashJoin;

import cascading.flow.FlowProcess;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.tuple.util.TupleViews;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JoinClosure extends JoinerClosure {

	private Tuple2<Tuple, Tuple[]> tupleJoinList;
	private SingleTupleIterator[] iterators;

	private Tuple joinedKeysTuple = new Tuple();
	private final Tuple emptyKeyTuple;
	private final Tuple[] keyTuples;
	private final TupleBuilder joinedKeysTupleBuilder;

	public JoinClosure(FlowProcess flowProcess, Fields[] joinFields, Fields[] valueFields) {
		super(flowProcess, joinFields, valueFields);
		this.emptyKeyTuple = Tuple.size( joinFields[0].size() );
		this.keyTuples = new Tuple[joinFields.length];
		this.joinedKeysTupleBuilder = makeJoinedBuilder( joinFields );
	}

	public void reset(Tuple2<Tuple, Tuple[]> tupleJoinList) {
		this.tupleJoinList = tupleJoinList;

		if(this.iterators == null) {
			this.iterators = new SingleTupleIterator[tupleJoinList.f1.length];
			for(int i=0; i < iterators.length; i++) {
				iterators[i] = new SingleTupleIterator();
			}
		}
	}

	@Override
	public int size() {
		return joinFields.length;
	}

	@Override
	public Iterator<Tuple> getIterator(int pos) {

		iterators[pos].reset(tupleJoinList.f1[pos]);
		return iterators[pos];
	}

	@Override
	public boolean isEmpty(int pos) {
		return this.tupleJoinList.f1[pos] == null;
	}

	@Override
	public Tuple getGroupTuple(Tuple keysTuple) {
		Tuples.asModifiable(joinedKeysTuple);

		for( int i = 0; i < this.keyTuples.length; i++ ) {
			keyTuples[i] = this.tupleJoinList.f1[i] == null ? emptyKeyTuple : tupleJoinList.f0;
		}

		joinedKeysTuple = joinedKeysTupleBuilder.makeResult( keyTuples );

		return joinedKeysTuple;
	}

	interface TupleBuilder {
		Tuple makeResult(Tuple[] tuples);
	}

	private TupleBuilder makeJoinedBuilder( final Fields[] joinFields )
	{
		final Fields[] fields = isSelfJoin() ? new Fields[ size() ] : joinFields;

		if( isSelfJoin() ) {
			Arrays.fill(fields, 0, fields.length, joinFields[0]);
		}

		return new TupleBuilder()
		{
			Tuple result = TupleViews.createComposite(fields);

			@Override
			public Tuple makeResult( Tuple[] tuples )
			{
				return TupleViews.reset( result, tuples );
			}
		};
	}

	private class SingleTupleIterator implements Iterator<Tuple> {

		private Tuple t;

		public void reset(Tuple t) {
			this.t = t;
		}

		@Override
		public boolean hasNext() {
			return t != null;
		}

		@Override
		public Tuple next() {
			Tuple t = this.t;
			this.t = null;
			return t;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
