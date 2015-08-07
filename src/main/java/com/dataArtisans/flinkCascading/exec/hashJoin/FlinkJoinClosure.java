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

package com.dataArtisans.flinkCascading.exec.hashJoin;

import cascading.flow.FlowProcess;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;

public class FlinkJoinClosure extends JoinerClosure {

	private Tuple2<Tuple, Tuple[]> tupleJoinList;

	private SingleTupleIterator[] iterators;

	public FlinkJoinClosure(FlowProcess flowProcess, Fields[] joinFields, Fields[] valueFields) {
		super(flowProcess, joinFields, valueFields);
	}

	public void reset(Tuple2<Tuple, Tuple[]> tupleJoinList) {
		this.tupleJoinList = tupleJoinList;

		if(this.iterators == null) {
			this.iterators = new SingleTupleIterator[tupleJoinList.f1.length+1];
			for(int i=0; i < iterators.length; i++) {
				iterators[i] = new SingleTupleIterator();
			}
		}

		iterators[0].reset(tupleJoinList.f0);
		for(int i=1; i < iterators.length; i++) {
			iterators[i].reset(tupleJoinList.f1[i-1]);
		}

	}

	@Override
	public int size() {
		return joinFields.length;
	}

	@Override
	public Iterator<Tuple> getIterator(int pos) {
		return iterators[pos];
	}

	@Override
	public boolean isEmpty(int pos) {
		return false;
	}

	@Override
	public Tuple getGroupTuple(Tuple keysTuple) {
		throw new UnsupportedOperationException();
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
	}
}
