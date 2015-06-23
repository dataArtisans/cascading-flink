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
import cascading.tuple.util.TupleBuilder;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class KeyPeekingIterator implements Iterator<Tuple> {

	private final Iterator<Tuple> values;
	private final TupleBuilder keyBuilder;

	private Tuple peekedValue;
	private Tuple peekedKey;

	public KeyPeekingIterator(Iterator<Tuple> values, TupleBuilder keyBuilder) {
		this.values = values;
		this.keyBuilder = keyBuilder;
	}

	public Tuple peekNextKey() {
		if(peekedValue == null && values.hasNext()) {
			peekedValue = values.next();
			peekedKey = keyBuilder.makeResult(peekedValue, peekedKey);
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
	public Tuple next() {

		if(peekedValue != null) {
			Tuple v = peekedValue;
			peekedValue = null;
			peekedKey = null;
			return v;
		}
		else {
			return values.next();
		}
	}
}
