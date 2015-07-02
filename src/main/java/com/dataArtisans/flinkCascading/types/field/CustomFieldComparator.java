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

package com.dataArtisans.flinkCascading.types.field;

import cascading.tuple.Hasher;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class CustomFieldComparator extends TypeComparator<Comparable> {

	private static final long serialVersionUID = 1L;

	private final boolean ascending;

	private final Comparator<Comparable> comparator;

	private final Hasher hasher;

	private TypeSerializer<Comparable> serializer;

	private transient Comparable ref;

	private transient Comparable tmpRef;

	@SuppressWarnings("rawtypes")
	private final TypeComparator[] cArray = new TypeComparator[] {this};

	public CustomFieldComparator(boolean ascending, Comparator<Comparable> comparator, TypeSerializer<Comparable> serializer) {
		this.ascending = ascending;
		this.comparator = comparator;
		this.hasher = initializeHasher(comparator);
		this.serializer = serializer;
	}

	private CustomFieldComparator(CustomFieldComparator toClone) {
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
			return new DefaultHasher();
		}
	}

	public int hash(Comparable record) {
		return this.hasher.hashCode(record);
	}

	public void setReference(Comparable toCompare) {
		if(toCompare == null) {
			this.ref = null;
		}
		else {
			this.ref = this.serializer.copy(toCompare);
		}
	}

	public boolean equalToReference(Comparable candidate) {
		return this.comparator.compare(candidate, this.ref) == 0;
	}

	public int compareToReference(TypeComparator<Comparable> referencedComparator) {
		Comparable otherRef = ((CustomFieldComparator)referencedComparator).ref;

		int cmp = this.comparator.compare(otherRef, this.ref);
		return this.ascending?cmp:-cmp;
	}

	public int compare(Comparable first, Comparable second) {
		int cmp;

		cmp = this.comparator.compare(first, second);
		return this.ascending?cmp:-cmp;
	}

	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		if(this.ref == null) {
			this.ref = this.serializer.createInstance();
		}

		if(this.tmpRef == null) {
			this.tmpRef = this.serializer.createInstance();
		}

		this.ref = this.serializer.deserialize(this.ref, firstSource);
		this.tmpRef = this.serializer.deserialize(this.tmpRef, secondSource);
		int cmp = this.comparator.compare(this.ref, this.tmpRef);
		return this.ascending?cmp:-cmp;
	}

	public TypeComparator<Comparable> duplicate() {
		return new CustomFieldComparator(this);
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
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public void putNormalizedKey(Comparable record, MemorySegment target, int offset, int numBytes) {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public boolean invertNormalizedKey() {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	public void writeWithKeyNormalization(Comparable record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	public Comparable readWithKeyDenormalization(Comparable reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported.");
	}

	private static class DefaultHasher implements Hasher, Serializable {

		@Override
		public int hashCode(Object value) {
			if(value == null) {
				return 1;
			}
			else {
				return value.hashCode();
			}
		}
	}

}
