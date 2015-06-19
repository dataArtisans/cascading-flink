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

package com.dataArtisans.flinkCascading_old.exec;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Iterator;

public class FlinkArgumentsIterator implements Iterator<TupleEntry> {

	private Iterator<Tuple3<Tuple, Tuple, Tuple>> flinkIterator;
	private TupleEntry argumentsEntry;
	private TupleBuilder argumentsBuilder;

	private Tuple3<Tuple, Tuple, Tuple> next;
	private Tuple key;

	private TupleBuilderCollector col = null;

	public FlinkArgumentsIterator(Iterable<Tuple3<Tuple, Tuple, Tuple>> vals, TupleEntry argumentsEntry, TupleBuilder argumentsBuilder) {
		this.flinkIterator = vals.iterator();
		this.argumentsEntry = argumentsEntry;
		this.argumentsBuilder = argumentsBuilder;

		next = this.flinkIterator.next();
		key = new Tuple(next.f0);
	}

	public void setTupleBuildingCollector(TupleBuilderCollector col) {
		this.col = col;
	}

	public Tuple getKey() {
		return key;
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public TupleEntry next() {

		Tuple3<Tuple, Tuple, Tuple> cur = next;
		if(flinkIterator.hasNext()) {
			next = flinkIterator.next();
		} else {

			// TODO (see BufferEveryWindow)
//			if( !call.isRetainValues() ) {
//				tupleEntry.setTuple(valueNulledTuple); // null out footer entries
//			}

			next = null;
		}

		if(col != null) {
			this.col.setInTuple(cur.f2);
		}

		argumentsEntry.setTuple( argumentsBuilder.makeResult( cur.f2, null ) );
		return argumentsEntry;

	}
}
