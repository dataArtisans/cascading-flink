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

package com.dataartisans.flink.cascading.types.tuple;

import cascading.tuple.Tuple;
import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.List;

public class UnknownTupleComparator extends CompositeTypeComparator<Tuple> {

	private static final long serialVersionUID = 1L;
	private static final int[] HASH_SALT = new int[]{73, 79, 97, 113, 131, 197, 199, 311, 337, 373, 719, 733, 919, 971, 991, 1193, 1931, 3119, 3779, 7793, 7937, 9311, 9377, 11939, 19391, 19937, '酏', '飏', 71993, 91193, 93719, 93911};

	private boolean areKeysAbs = false;
	private final int[] keyPositions;
	private final TypeComparator[] comparators;
	private final TypeSerializer serializer;

	private final int[] normalizedKeyLengths;
	private final int numLeadingNormalizableKeys;
	private final int normalizableKeyPrefixLen;
	private final boolean invertNormKey;

	private Object[] fields1;
	private Object[] fields2;
	private boolean[] nullFields1;
	private boolean[] nullFields2;
	private int maxKey;

	public UnknownTupleComparator(int[] keyPositions, TypeComparator<?>[] comparators, TypeSerializer<?> serializer) {

		this.keyPositions = keyPositions;
		this.comparators = comparators;
		this.serializer = serializer;

		// set up auxiliary fields for normalized key support
		this.normalizedKeyLengths = new int[keyPositions.length];
		int nKeys = 0;
		int nKeyLen = 0;
		boolean inverted = false;

		for (int i = 0; i < this.keyPositions.length; i++) {
			TypeComparator<?> k = this.comparators[i];

			// as long as the leading keys support normalized keys, we can build up the composite key
			if (k.supportsNormalizedKey()) {
				if (i == 0) {
					// the first comparator decides whether we need to invert the key direction
					inverted = k.invertNormalizedKey();
				}
				else if (k.invertNormalizedKey() != inverted) {
					// if a successor does not agree on the inversion direction, it cannot be part of the normalized key
					break;
				}

				nKeys++;
				final int len = k.getNormalizeKeyLen();
				if (len < 0) {
					throw new RuntimeException("Comparator " + k.getClass().getName() + " specifies an invalid length for the normalized key: " + len);
				}
				this.normalizedKeyLengths[i] = len;
				nKeyLen += len;

				if (nKeyLen < 0) {
					// overflow, which means we are out of budget for normalized key space anyways
					nKeyLen = Integer.MAX_VALUE;
					break;
				}
			} else {
				break;
			}
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
		this.invertNormKey = inverted;
	}

	private UnknownTupleComparator(UnknownTupleComparator toClone) {
		this(toClone.keyPositions, cloneComparators(toClone.comparators), toClone.serializer.duplicate());
	}

	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public int hash(Tuple value) {

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, value.size());
			areKeysAbs = true;
		}

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

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, toCompare.size());
			areKeysAbs = true;
		}

		for (int i = 0; i < this.keyPositions.length; i++) {
			this.comparators[i].setReference(toCompare.getObject(keyPositions[i]));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equalToReference(Tuple candidate) {

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, candidate.size());
			areKeysAbs = true;
		}

		for (int i = 0; i < this.keyPositions.length; i++) {
			if (!this.comparators[i].equalToReference(candidate.getObject(keyPositions[i]))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int compareToReference(TypeComparator<Tuple> typeComparator) {

		UnknownTupleComparator other = (UnknownTupleComparator)typeComparator;

		for(int i=0; i<this.keyPositions.length; i++) {
			int cmp = this.comparators[i].compareToReference(other.comparators[i]);
			if(cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}

	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {

		int arity1 = firstSource.readInt();
		int arity2 = secondSource.readInt();

		if(this.fields1 == null) {
			// first time.
			this.fields1 = new Object[arity1];
			this.fields2 = new Object[arity2];
			this.nullFields1 = new boolean[arity1];
			this.nullFields2 = new boolean[arity2];
		}

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, arity1);
			areKeysAbs = true;
			maxKey = 0;
			for(int i=0; i<keyPositions.length; i++) {
				maxKey = maxKey > keyPositions[i] ? maxKey : keyPositions[i];
			}
		}

		NullMaskSerDeUtils.readNullMask(nullFields1, arity1, firstSource);
		NullMaskSerDeUtils.readNullMask(nullFields2, arity2, secondSource);

		for (int i=0; i <= maxKey; i++) {
			if(!nullFields1[i]) {
				fields1[i] = serializer.deserialize(fields1[i], firstSource);
			}
			else {
				fields1[i] = null;
			}
			if(!nullFields2[i]) {
				fields2[i] = serializer.deserialize(fields2[i], secondSource);
			}
			else {
				fields2[i] = null;
			}
		}


		for (int i = 0; i < keyPositions.length; i++) {
			int cmp = comparators[i].compare(fields1[keyPositions[i]], fields2[keyPositions[i]]);

			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(Tuple first, Tuple second) {

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, first.size());
			areKeysAbs = true;
		}

		for (int i=0; i<keyPositions.length; i++) {
			int cmp = comparators[i].compare(first.getObject(keyPositions[i]), second.getObject(keyPositions[i]));

			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return this.numLeadingNormalizableKeys > 0;
	}

	@Override
	public boolean invertNormalizedKey() {
		return this.invertNormKey;
	}

	@Override
	public int getNormalizeKeyLen() {
		return this.normalizableKeyPrefixLen;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return this.numLeadingNormalizableKeys < this.keyPositions.length ||
				this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
				this.normalizableKeyPrefixLen > keyBytes;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putNormalizedKey(Tuple value, MemorySegment target, int offset, int numBytes) {

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, value.size());
			areKeysAbs = true;
		}

		int i = 0;
		for (; i < this.numLeadingNormalizableKeys && numBytes > 0; i++) {
			int len = this.normalizedKeyLengths[i];
			len = numBytes >= len ? len : numBytes;
			this.comparators[i].putNormalizedKey(value.getObject(keyPositions[i]), target, offset, len);
			numBytes -= len;
			offset += len;
		}
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {

		if(!areKeysAbs) {
			makeKeysAbs(keyPositions, ((Tuple)record).size());
			areKeysAbs = true;
		}

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
		return new UnknownTupleComparator(this);
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(Tuple objects, DataOutputView dataOutputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not suppported for Cascading tuples");
	}

	@Override
	public Tuple readWithKeyDenormalization(Tuple objects, DataInputView dataInputView) throws IOException {
		throw new UnsupportedOperationException("Normalized keys not suppported for Cascading tuples");
	}

	private static TypeComparator[] cloneComparators(TypeComparator[] comps) {

		TypeComparator[] clonedComps = new TypeComparator[comps.length];
		for(int i = 0; i < comps.length; i++) {
			clonedComps[i] = comps[i].duplicate();
		}
		return clonedComps;
	}

	private static TypeSerializer[] cloneSerializers(TypeSerializer[] serializers) {

		TypeSerializer[] clonedSerializers = new TypeSerializer[serializers.length];
		for(int i = 0; i < serializers.length; i++) {
			clonedSerializers[i] = serializers[i].duplicate();
		}
		return clonedSerializers;
	}

	private void makeKeysAbs(int[] keyPos, int length) {

		for(int i=0; i<keyPos.length; i++) {

			if(keyPos[i] < 0) {
				keyPos[i] = length + keyPos[i];
			}
		}
	}

}
