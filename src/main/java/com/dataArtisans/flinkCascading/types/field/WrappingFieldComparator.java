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

package com.dataArtisans.flinkCascading.types.field;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

public class WrappingFieldComparator<T extends Comparable<T>> extends FieldComparator<T> {

	private static final long serialVersionUID = 1L;

	private static final byte NULL_FLAG =     (byte)0x00;
	private static final byte NOT_NULL_FLAG = (byte)0xFF;

	private final TypeComparator wrappedComparator;

	public WrappingFieldComparator(TypeComparator<T> wrappedComparator, boolean ascending, TypeSerializer<T> serializer, Class<T> type) {
		super(ascending, serializer, type);
		this.wrappedComparator = wrappedComparator;
	}

	public WrappingFieldComparator(WrappingFieldComparator toClone) {
		this(toClone.wrappedComparator.duplicate(),
				toClone.ascending, toClone.serializer, toClone.type);
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
