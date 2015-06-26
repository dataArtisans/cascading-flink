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
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.Comparator;

public class CustomCascadingFieldComparator<T> extends TypeComparator<T> {

	private static final long serialVersionUID = 1L;

	private final boolean ascending;

	private final Comparator<T> comparator;

	private final Hasher hasher;

	private TypeSerializer<T> serializer;

	private transient T reference;

	private transient T tmpReference;

	@SuppressWarnings("rawtypes")
	private final TypeComparator[] cArray = new TypeComparator[] {this};

	public CustomCascadingFieldComparator(boolean ascending, Comparator<T> comparator, TypeSerializer<T> serializer) {
		this.ascending = ascending;
		this.comparator = comparator;
		this.hasher = initializeHasher(comparator);
		this.serializer = serializer;
	}

	private CustomCascadingFieldComparator(CustomCascadingFieldComparator<T> toClone) {
		this.ascending = toClone.ascending;
		this.comparator = toClone.comparator;
		this.hasher = toClone.hasher;
		this.serializer = toClone.serializer.duplicate();
	}

	private static Hasher initializeHasher(Comparator comparator) {

		if(comparator instanceof Hasher) {
			return (Hasher)comparator;
		}
		else {
			return new Hasher() {

				@Override
				public int hashCode(Object value) {
					return value.hashCode();
				}
			};
		}
	}

	public int hash(T record) {
		return this.hasher.hashCode(record);
	}

	public void setReference(T toCompare) {
		this.reference = this.serializer.copy(toCompare);
	}

	public boolean equalToReference(T candidate) {
		return this.comparator.compare(candidate, this.reference) == 0;
	}

	public int compareToReference(TypeComparator<T> referencedComparator) {
		T otherRef = (T)((CustomCascadingFieldComparator)referencedComparator).reference;
		int cmp = this.comparator.compare(otherRef, this.reference);
		return this.ascending?cmp:-cmp;
	}

	public int compare(T first, T second) {
		return this.comparator.compare(first, second);
	}

	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		if(this.reference == null) {
			this.reference = this.serializer.createInstance();
		}

		if(this.tmpReference == null) {
			this.tmpReference = this.serializer.createInstance();
		}

		this.reference = this.serializer.deserialize(this.reference, firstSource);
		this.tmpReference = this.serializer.deserialize(this.tmpReference, secondSource);
		int cmp = this.comparator.compare(this.reference, this.tmpReference);
		return this.ascending?cmp:-cmp;
	}

	public TypeComparator<T> duplicate() {
		return new CustomCascadingFieldComparator<T>(this);
	}

	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	public TypeComparator[] getFlatComparators() {
		return this.cArray;
	}

	// Normalized keys not supported for custom comparators

	public boolean supportsNormalizedKey() {
		return false;
	}

	public int getNormalizeKeyLen() {
		return -1;
	}

	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < this.getNormalizeKeyLen();
	}

	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public boolean invertNormalizedKey() {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}


}
