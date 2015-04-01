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

package com.dataArtisans.flinkCascading.types;

import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleHasher;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class CascadingTupleComparator extends TypeComparator<Tuple> {

	private final Comparator[] comparators;
	private final TupleHasher hasher;
	private final boolean ascending;

	private TypeSerializer<Tuple> serializer;

	private Tuple reference;
	private Tuple tmpReference;

	public CascadingTupleComparator(boolean ascending, TypeSerializer<Tuple> serializer) {
		this.comparators = null;
		this.hasher = null;
		this.ascending = ascending;
		this.serializer = serializer;
	}

	public CascadingTupleComparator(boolean ascending, Comparator[] comparators, TypeSerializer<Tuple> serializer) {
		this.ascending = ascending;
		this.comparators = comparators;
		if(comparators != null) {
			this.hasher = new TupleHasher(new DefaultObjectHasher(), comparators);
		}
		else {
			this.hasher = null;
		}
		this.serializer = serializer;
	}

	@Override
	public int hash(Tuple t) {

		if(hasher == null) {
			return t.hashCode();
		}
		else {
			return this.hasher.hashCode(t);
		}
	}

	@Override
	public void setReference(Tuple t) {
		this.reference = t;
	}

	@Override
	public boolean equalToReference(Tuple t) {

		if(comparators == null) {
			return reference.compareTo(t) == 0;
		}
		else {
			return reference.compareTo(this.comparators, t) == 0;
		}
	}

	@Override
	public int compareToReference(TypeComparator<Tuple> typeComparator) {

		Tuple otherRef = ((CascadingTupleComparator) typeComparator).reference;
		return compare(this.reference, otherRef);
	}

	@Override
	public int compare(Tuple t1, Tuple t2) {

		if(comparators == null) {
			return t1.compareTo(t2);
		}
		else {
			return t1.compareTo(this.comparators, t2);
		}
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {

		if (this.reference == null) {
			this.reference = this.serializer.createInstance();
		}

		if (this.tmpReference == null) {
			this.tmpReference = this.serializer.createInstance();
		}

		this.reference = this.serializer.deserialize(this.reference, firstSource);
		this.tmpReference = this.serializer.deserialize(this.tmpReference, secondSource);

		int cmp = this.reference.compareTo(this.tmpReference);
		return this.ascending ? cmp : -cmp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 0;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putNormalizedKey(Tuple objects, MemorySegment memorySegment, int i, int i1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeWithKeyNormalization(Tuple objects, DataOutputView dataOutputView) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Tuple readWithKeyDenormalization(Tuple objects, DataInputView dataInputView) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean invertNormalizedKey() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TypeComparator<Tuple> duplicate() {
		return new CascadingTupleComparator(ascending, comparators, this.serializer.duplicate());
	}

	@Override
	public int extractKeys(Object t, Object[] target, int index) {
		target[index] = t;
		return 1;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[]{this};
	}

	public static class DefaultObjectHasher implements Comparator<Object>, Hasher<Object>, Serializable {

		@Override
		public int hashCode( Object value )
		{
			return value.hashCode();
		}

		@Override
		public int compare(Object o1, Object o2) {
			throw new UnsupportedOperationException();
		}
	}

}
