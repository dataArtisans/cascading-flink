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

package com.dataartisans.flink.cascading.types.tuplearray;

import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.types.tuple.NullMaskSerDeUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;

public class TupleArraySerializer extends TypeSerializer<Tuple[]> {

	private final int length;
	private final int fillLength;
	private final TypeSerializer<Tuple>[] tupleSerializers;

	private final boolean[] nullFields;

	public TupleArraySerializer(int length, TypeSerializer<Tuple>[] tupleSerializers) {

		this.length = length;
		this.fillLength = tupleSerializers.length;
		this.tupleSerializers = tupleSerializers;
		this.nullFields = new boolean[this.fillLength];
	}

	@Override
	public Tuple[] createInstance() {
		return new Tuple[this.length];
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Tuple[]> duplicate() {

		TypeSerializer<Tuple>[] serializerCopies = new TypeSerializer[this.fillLength];
		for(int i=0; i<this.fillLength; i++) {
			serializerCopies[i] = this.tupleSerializers[i].duplicate();
		}
		return new TupleArraySerializer(this.length, serializerCopies);
	}

	@Override
	public Tuple[] copy(Tuple[] from) {

		Tuple[] copy = new Tuple[this.length];
		for(int i=0; i<this.fillLength; i++) {
			if(from[i] != null) {
				copy[i] = this.tupleSerializers[i].copy(from[i]);
			}
			else {
				copy[i] = null;
			}
		}
		return copy;
	}

	@Override
	public Tuple[] copy(Tuple[] from, Tuple[] reuse) {
		for(int i=0; i<this.fillLength; i++) {
			if(from[i] != null) {
				reuse[i] = this.tupleSerializers[i].copy(from[i]);
			}
			else {
				reuse[i] = null;
			}
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(Tuple[] tuples, DataOutputView target) throws IOException {

		// write null mask
		NullMaskSerDeUtils.writeNullMask(tuples, fillLength, target);

		for (int i = 0; i < fillLength; i++) {
			if(tuples[i] != null) {
				tupleSerializers[i].serialize(tuples[i], target);
			}
		}
	}

	@Override
	public Tuple[] deserialize(DataInputView source) throws IOException {

		// read null mask
		NullMaskSerDeUtils.readNullMask(this.nullFields, this.fillLength, source);

		// read non-null fields
		Tuple[] tuples = new Tuple[this.length];
		for (int i = 0; i < this.fillLength; i++) {

			if(!this.nullFields[i]) {
				tuples[i] = tupleSerializers[i].deserialize(source);
			}
		}
		return tuples;
	}

	@Override
	public Tuple[] deserialize(Tuple[] reuse, DataInputView source) throws IOException {

		// read null mask
		NullMaskSerDeUtils.readNullMask(this.nullFields, this.fillLength, source);

		// read non-null fields
		for (int i = 0; i < this.fillLength; i++) {

			if(!this.nullFields[i]) {
				reuse[i] = tupleSerializers[i].deserialize(source);
			}
			else {
				reuse[i] = null;
			}
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

		// read and copy null mask
		NullMaskSerDeUtils.readAndCopyNullMask(nullFields, this.fillLength, source, target);

		// copy non-null fields
		for (int i = 0; i < this.fillLength; i++) {
			if(!this.nullFields[i]) {
				tupleSerializers[i].copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleArraySerializer) {
			TupleArraySerializer other = (TupleArraySerializer) obj;

			return other.canEqual(this) &&
					this.length == other.length &&
					Arrays.equals(this.tupleSerializers, other.tupleSerializers);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * this.length + Arrays.hashCode(this.tupleSerializers);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleArraySerializer;
	}

}
