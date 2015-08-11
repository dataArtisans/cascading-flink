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

package com.dataArtisans.flinkCascading.types.tuple;

import cascading.tuple.Tuple;
import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.List;

public class TupleComparator extends CompositeTypeComparator<Tuple> {

	private static final long serialVersionUID = 1L;
	private static final int[] HASH_SALT = new int[]{73, 79, 97, 113, 131, 197, 199, 311, 337, 373, 719, 733, 919, 971, 991, 1193, 1931, 3119, 3779, 7793, 7937, 9311, 9377, 11939, 19391, 19937, '酏', '飏', 71993, 91193, 93719, 93911};

	private final int tupleLength;
	private int[] keyPositions;
	private TypeComparator[] comparators;
	private TypeSerializer[] serializers;

	private Object[] fields1;
	private Object[] fields2;


	public TupleComparator(int[] keyPositions, TypeComparator<?>[] comparators, TypeSerializer<?>[] serializers, int tupleLength) {

		this.keyPositions = keyPositions;
		this.comparators = comparators;
		this.serializers = serializers;
		this.tupleLength = tupleLength;

		fields1 = new Object[serializers.length];
		fields2 = new Object[serializers.length];
	}

	private TupleComparator(TupleComparator toClone) {
		this(toClone.keyPositions, toClone.comparators, toClone.serializers, toClone.tupleLength);
	}

	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public int hash(Tuple value) {
		int code = this.comparators[0].hash(value.getObject(keyPositions[0]));

		for (int i = 1; i < this.keyPositions.length; i++) {
			code *= HASH_SALT[i & 0x1F]; // salt code with (i % HASH_SALT.length)-th salt component
			code += this.comparators[i].hash(value.getObject(keyPositions[i]));
		}
		return code;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setReference(Tuple toCompare) {
		for (int i = 0; i < this.keyPositions.length; i++) {
			this.comparators[i].setReference(toCompare.getObject(this.keyPositions[i]));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equalToReference(Tuple candidate) {
		for (int i = 0; i < this.keyPositions.length; i++) {
			if (!this.comparators[i].equalToReference(candidate.getObject(this.keyPositions[i]))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int compareToReference(TypeComparator<Tuple> typeComparator) {

		TupleComparator other = (TupleComparator)typeComparator;

		for(int i=0; i<this.keyPositions.length; i++) {
			int cmp = this.comparators[i].compareToReference(other.comparators[i]);
			if(cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}

	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {

		int arity1 = this.tupleLength < 0 ? firstSource.readInt() : this.tupleLength;
		int arity2 = this.tupleLength < 0 ? secondSource.readInt() : this.tupleLength;

		// TODO more efficient via bit masks
		boolean[] nullFields1 = new boolean[arity1];
		boolean[] nullFields2 = new boolean[arity2];
		for(int i = 0; i <arity1; i++) {
			nullFields1[i] = firstSource.readBoolean();
		}
		for(int i=0; i <arity2; i++) {
			nullFields2[i] = secondSource.readBoolean();
		}

		for (int i=0; i < serializers.length; i++) {
			if(!nullFields1[i]) {
				fields1[i] = serializers[i].deserialize(fields1[i], firstSource);
			}
			else {
				fields1[i] = null;
			}
			if(!nullFields2[i]) {
				fields2[i] = serializers[i].deserialize(fields2[i], secondSource);
			}
			else {
				fields2[i] = null;
			}
		}

		for (int i = 0; i < keyPositions.length; i++) {
			int keyPos = keyPositions[i];
			int cmp = comparators[i].compare(fields1[keyPos], fields2[keyPos]);

			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(Tuple first, Tuple second) {

		for (int i=0; i<keyPositions.length; i++) {
			int keyPos = keyPositions[i];
			int cmp = comparators[i].compare(first.getObject(keyPos), second.getObject(keyPos));

			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
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

	@SuppressWarnings("unchecked")
	@Override
	public void putNormalizedKey(Tuple value, MemorySegment target, int offset, int numBytes) {
		throw new UnsupportedOperationException("Normalized keys not suppported for Cascading tuples");
	}

	@Override
	public void writeWithKeyNormalization(Tuple objects, DataOutputView dataOutputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not suppported for Cascading tuples");
	}

	@Override
	public Tuple readWithKeyDenormalization(Tuple objects, DataInputView dataInputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not suppported for Cascading tuples");
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		int localIndex = index;
		for(int i = 0; i < comparators.length; i++) {
			localIndex += comparators[i].extractKeys(((Tuple) record).getObject(keyPositions[i]), target, localIndex);
		}
		return localIndex - index;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return this.comparators;
	}

	@Override
	public void getFlatComparator(List<TypeComparator> list) {
		for(TypeComparator tc : this.comparators) {
			list.add(tc.duplicate());
		}
	}

	public TypeComparator<Tuple> duplicate() {
		return new TupleComparator(this);
	}

}
