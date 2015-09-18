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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class DefinedTupleSerializer extends TypeSerializer<Tuple> {

	private static final long serialVersionUID = 1L;

	private final boolean[] nullFields;
	private final TypeSerializer fieldSer;
	private final int length;

	public DefinedTupleSerializer(TypeSerializer fieldSer, int length) {
		this.fieldSer = fieldSer;
		this.length = length;
		this.nullFields = new boolean[this.length];
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public DefinedTupleSerializer duplicate() {
		return new DefinedTupleSerializer(this.fieldSer.duplicate(), length);
	}

	@Override
	public Tuple createInstance() {
		try {
			return Tuple.size(length);
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

		// write null mask
		NullMaskSerDeUtils.writeNullMask(value, target);

		for (int i = 0; i < value.size(); i++) {
			Object o = value.getObject(i);
			if(o != null) {
				fieldSer.serialize(o, target);
			}
		}
	}

	@Override
	public Tuple deserialize(DataInputView source) throws IOException {

		// read null mask
		NullMaskSerDeUtils.readNullMask(this.nullFields, this.length, source);

		// read non-null fields
		Tuple tuple = Tuple.size(this.length);
		for (int i = 0; i < this.length; i++) {
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

		// read null mask
		NullMaskSerDeUtils.readNullMask(nullFields, this.length, source);

		for (int i = 0; i < this.length; i++) {
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

		// read and copy null mask
		NullMaskSerDeUtils.readAndCopyNullMask(nullFields, this.length, source, target);

		// copy non-null fields
		for (int i = 0; i < this.length; i++) {
			if(!this.nullFields[i]) {
				fieldSer.copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DefinedTupleSerializer) {
			DefinedTupleSerializer other = (DefinedTupleSerializer) obj;

			return other.canEqual(this) &&
					length == other.length &&
					fieldSer.equals(other.fieldSer);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * this.fieldSer.hashCode() + length;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof DefinedTupleSerializer;
	}

}
