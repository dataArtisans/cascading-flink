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

import cascading.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Iterator;

public class FlinkUnwrappingIterator<F1, F2> implements Iterator<Tuple> {

	private Iterator<Tuple3<F1, F2, Tuple>> flinkIterator;


	public FlinkUnwrappingIterator(Iterable<Tuple3<F1, F2, Tuple>> vals) {
		this(vals.iterator());
	}

	public FlinkUnwrappingIterator(Iterator<Tuple3<F1, F2, Tuple>> vals) {
		this.flinkIterator = vals;
	}

	@Override
	public boolean hasNext() {
		return flinkIterator.hasNext();
	}

	@Override
	public Tuple next() {

		return flinkIterator.next().f2;
	}
}
