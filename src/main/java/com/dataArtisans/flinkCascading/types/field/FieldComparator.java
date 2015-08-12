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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

public class FieldComparator<T extends Comparable<T>> extends TypeComparator<T> {

	private static final long serialVersionUID = 1L;

	protected final boolean ascending;
	protected final Class<T> type;
	protected TypeSerializer<T> serializer;

	private transient T ref;
	private transient T tmpRef;

	public FieldComparator(boolean ascending, TypeSerializer<T> serializer, Class<T> type) {
		this.ascending = ascending;
		this.serializer = serializer;
		this.type = type;
	}

	public FieldComparator(FieldComparator toClone) {
		this(toClone.ascending, toClone.serializer, toClone.type);
	}

	@Override
	public int hash(T t) {
		if(t == null) {
			return 1;
		}
		else {
			return t.hashCode();
		}
	}

	@Override
	public void setReference(T t) {
		if(t == null) {
			this.ref = null;
		}
		else {
			this.ref = (T)this.serializer.copy(t);
		}
	}

	@Override
	public boolean equalToReference(T t) {
		if(t != null && ref != null) {
			return t.equals(this.ref);
		}
		else if(t == null && ref == null) {
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public int compareToReference(TypeComparator<T> typeComparator) {
		FieldComparator other = (FieldComparator)typeComparator;
		int cmp;

		if(this.ref != null && other.ref != null) {
			cmp = other.ref.compareTo(this.ref);

		}
		else if(this.ref == null && other.ref == null) {
			cmp = 0;
		}
		else if(this.ref == null && other.ref != null) {
			cmp = -1;
		}
		else {
			cmp = 1;
		}
		return this.ascending?cmp:-cmp;
	}

	@Override
	public int compare(T t1, T t2) {
		int cmp;

		if(t1 != null && t2 != null) {
			cmp = t1.compareTo(t2);
		}
		else if(t1 == null && t2 == null) {
			cmp = 0;
		}
		else if(t1 == null && t2 != null) {
			cmp = -1;
		}
		else {
			cmp = 1;
		}
		return this.ascending?cmp:-cmp;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		if(this.ref == null) {
			this.ref = this.serializer.createInstance();
		}

		if(this.tmpRef == null) {
			this.tmpRef = this.serializer.createInstance();
		}

		this.ref = this.serializer.deserialize(this.ref, firstSource);
		this.tmpRef = this.serializer.deserialize(this.tmpRef, secondSource);
		int cmp = this.ref.compareTo(this.tmpRef);
		return this.ascending?cmp:-cmp;
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
		return -1;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int i) {
		return false;
	}

	@Override
	public void putNormalizedKey(T t, MemorySegment memorySegment, int i, int i1) {
		throw new UnsupportedOperationException("Normalized keys not supported for Cascading fields.");
	}

	@Override
	public void writeWithKeyNormalization(T t, DataOutputView dataOutputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported for Cascading fields.");
	}

	@Override
	public T readWithKeyDenormalization(T t, DataInputView dataInputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported for Cascading fields.");
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new FieldComparator<T>(this);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int idx) {
		target[idx] = record;
		return 1;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[] {this};
	}

}
