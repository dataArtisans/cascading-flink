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

package com.dataartisans.flink.cascading.types.field;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

public class WrappingFieldComparator<T extends Comparable<T>> extends TypeComparator<T> {

	private static final long serialVersionUID = 1L;

	private static final byte NULL_FLAG =     (byte)0x00;
	private static final byte NOT_NULL_FLAG = (byte)0xFF;

	private final Class<T> type;
	private final TypeComparator<T> wrappedComparator;
	private final boolean ascending;

	private transient boolean refNull = true;

	public WrappingFieldComparator(TypeComparator<T> wrappedComparator, boolean ascending,  Class<T> type) {
		this.type = type;
		this.ascending = ascending;
		this.wrappedComparator = wrappedComparator;
	}

	public WrappingFieldComparator(WrappingFieldComparator<T> toClone) {
		this(toClone.wrappedComparator.duplicate(), toClone.ascending, toClone.type);
	}

	@Override
	public int hash(T t) {
		return (t == null) ? 1 : this.wrappedComparator.hash(t);
	}

	@Override
	public void setReference(T t) {
		if(t == null) {
			this.refNull = true;
		}
		else {
			this.wrappedComparator.setReference(t);
			this.refNull = false;
		}
	}

	@Override
	public boolean equalToReference(T t) {
		if(t != null && !this.refNull) {
			return this.wrappedComparator.equalToReference(t);
		}
		else if(t == null && this.refNull) {
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public int compareToReference(TypeComparator<T> typeComparator) {
		WrappingFieldComparator<T> other = (WrappingFieldComparator<T>)typeComparator;

		if(!this.refNull && !other.refNull) {
			return this.wrappedComparator.compareToReference(other.wrappedComparator);
		}
		else if(this.refNull && other.refNull) {
			return 0;
		}
		else if(this.refNull && !other.refNull) {
			return ascending ? -1 : 1;
		}
		else {
			return ascending ? 1 : -1;
		}
	}

	@Override
	public int compare(T t1, T t2) {
		if(t1 != null && t2 != null) {
			return this.wrappedComparator.compare(t1, t2);
		}
		else if(t1 == null && t2 == null) {
			return 0;
		}
		else if(t1 == null && t2 != null) {
			return ascending ? -1 : 1;
		}
		else {
			return ascending ? 1 : -1;
		}
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		return this.wrappedComparator.compareSerialized(firstSource, secondSource);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return this.wrappedComparator.supportsNormalizedKey();
	}

	@Override
	public boolean invertNormalizedKey() {
		return this.wrappedComparator.invertNormalizedKey();
	}

	@Override
	public int getNormalizeKeyLen() {
		int normalizeKeyLen = this.wrappedComparator.getNormalizeKeyLen();
		if(normalizeKeyLen != Integer.MAX_VALUE) {
			return normalizeKeyLen + 1;
		}
		else {
			return normalizeKeyLen;
		}
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int numBytes) {
		return this.wrappedComparator.isNormalizedKeyPrefixOnly(numBytes + 1);
	}

	@Override
	public void putNormalizedKey(T val, MemorySegment target, int offset, int numBytes) {
		if(val != null) {
			target.put(offset, NOT_NULL_FLAG);
			this.wrappedComparator.putNormalizedKey(val, target, offset + 1, numBytes - 1);
		}
		else {
			for(int i=0; i < numBytes; i++) {
				target.put(offset, NULL_FLAG);
			}
		}
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new WrappingFieldComparator<T>(this);
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

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(T t, DataOutputView dataOutputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported for Cascading fields.");
	}

	@Override
	public T readWithKeyDenormalization(T t, DataInputView dataInputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not supported for Cascading fields.");
	}

}
