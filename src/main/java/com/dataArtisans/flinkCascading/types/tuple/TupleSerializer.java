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

package com.dataArtisans.flinkCascading.types.tuple;

import cascading.flow.FlowException;
import cascading.tuple.Tuple;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class TupleSerializer extends TypeSerializer<Tuple> {

	private static final long serialVersionUID = 1L;
	private boolean[] nullFields;
	private TypeSerializer fieldSer;
	private final int length;

	public TupleSerializer(TypeSerializer fieldSer, int length) {
		this.fieldSer = fieldSer;
		this.length = length;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TupleSerializer duplicate() {
		return new TupleSerializer(this.fieldSer.duplicate(), length);
	}

	@Override
	public Tuple createInstance() {
		try {
			if(length > 0) {
				return Tuple.size(length);
			}
			else {
				return Tuple.size(0);
			}
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public Tuple copy(Tuple from) {
		Tuple target = Tuple.size(from.size());
		for (int i = 0; i < from.size(); i++) {
			Object copy = fieldSer.copy(from.getObject(i));
			target.set(i, copy);
		}
		return target;
	}

	@Override
	public Tuple copy(Tuple from, Tuple reuse) {

		for (int i = 0; i < from.size(); i++) {
			Object copy = fieldSer.copy(from.getObject(i), reuse.getObject(i));
			reuse.set(i, copy);
		}

		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Tuple value, DataOutputView target) throws IOException {

		if(this.length < 0) {
			// write length only if length is unknown
			target.writeInt(value.size());
		}
		else {
			// verify correct length of tuple
			if(value.size() != length) {
				throw new FlowException("Size of tuple "+value+" is not correspond to specified size ("+length+").");
			}
		}

		// write null mask
		TupleSerializer.writeNullMask(value, target);

		for (int i = 0; i < value.size(); i++) {
			Object o = value.getObject(i);
			if(o != null) {
				fieldSer.serialize(o, target);
			}
		}
	}

	@Override
	public Tuple deserialize(DataInputView source) throws IOException {

		// read length only if unknown
		int arity = this.length < 0 ? source.readInt() : this.length;

		// initialize or resize null fields if necessary
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		// read null mask
		TupleSerializer.readNullMask(this.nullFields, arity, source);

		// read non-null fields
		Tuple tuple = Tuple.size(arity);
		for (int i = 0; i < arity; i++) {
			Object field;
			if(!this.nullFields[i]) {
				field = fieldSer.deserialize(source);
			}
			else {
				field = null;
			}
			tuple.set(i, field);
		}

		return tuple;
	}

	@Override
	public Tuple deserialize(Tuple reuse, DataInputView source) throws IOException {

		// read length only if unknown
		int arity = this.length < 0 ? source.readInt() : this.length;

		// initialize or resize null fields if necessary
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}
		// resize Tuple if necessary
		if(reuse.size() != arity) {
			reuse = Tuple.size(arity);
		}

		// read null mask
		TupleSerializer.readNullMask(nullFields, arity, source);

		for (int i = 0; i < arity; i++) {
			Object field;
			if(!this.nullFields[i]) {
				field = fieldSer.deserialize(source);
			}
			else {
				field = null;
			}
			reuse.set(i, field);
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

		// read length only if unknown
		int arity = this.length < 0 ? source.readInt() : this.length;

		// write length if necessary
		if(this.length < 0) {
			target.writeInt(arity);
		}

		// initialize or resize nullFields if necessary
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		// read and copy null mask
		TupleSerializer.readAndCopyNullMask(nullFields, arity, source, target);

		// copy non-null fields
		for (int i = 0; i < arity; i++) {
			if(!this.nullFields[i]) {
				fieldSer.copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		return (o instanceof TupleSerializer);
	}

	public static void writeNullMask(
			Tuple t, DataOutputView target) throws IOException{

		final int length = t.size();
		int b;
		int bytePos;

		for(int fieldPos = 0; fieldPos < length; ) {
			b = 0x00;
			// set bits in byte
			for(bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				b = b << 1;
				// set bit if field is null
				if(t.getObject(fieldPos) == null) {
					b |= 0x01;
				}
			}
			// shift bits if last byte is not completely filled
			for(; bytePos < 8; bytePos++) {
				b = b << 1;
			}
			// write byte
			target.writeByte(b);
		}
	}

	public static void readNullMask(
			boolean[] mask, int length, DataInputView source) throws IOException {

		for(int fieldPos = 0; fieldPos < length; ) {
			// read byte
			int b = source.readUnsignedByte();
			for(int bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				mask[fieldPos] = (b & 0x80) > 0;
				b = b << 1;
			}
		}
	}

	public static void readAndCopyNullMask(
			boolean[] mask, int length, DataInputView source, DataOutputView target) throws IOException {

		for(int fieldPos = 0; fieldPos < length; ) {
			// read byte
			int b = source.readUnsignedByte();
			// copy byte
			target.writeByte(b);
			for(int bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				mask[fieldPos] = (b & 0x80) > 0;
				b = b << 1;
			}
		}
	}

}
