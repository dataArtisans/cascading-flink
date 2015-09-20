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

public class UnknownTupleSerializer extends TypeSerializer<Tuple> {

	private static final long serialVersionUID = 1L;

	private boolean[] nullFields;
	private final TypeSerializer fieldSer;

	public UnknownTupleSerializer(TypeSerializer fieldSer) {
		this.fieldSer = fieldSer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public UnknownTupleSerializer duplicate() {
		return new UnknownTupleSerializer(this.fieldSer.duplicate());
	}

	@Override
	public Tuple createInstance() {
		try {
			return Tuple.size(0);
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

		Tuple tuple = getReuseOrNew(reuse, from.size());

		for (int i = 0; i < from.size(); i++) {
			Object copy = fieldSer.copy(from.getObject(i), tuple.getObject(i));
			tuple.set(i, copy);
		}

		return tuple;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Tuple value, DataOutputView target) throws IOException {

		// write length
		target.writeInt(value.size());

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

		// read length
		int arity = source.readInt();

		// initialize or resize null fields if necessary
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		// read null mask
		NullMaskSerDeUtils.readNullMask(this.nullFields, arity, source);

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

		// read length
		int arity = source.readInt();

		// initialize or resize null fields if necessary
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		Tuple tuple = getReuseOrNew(reuse, arity);

		// read null mask
		NullMaskSerDeUtils.readNullMask(nullFields, arity, source);

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
	public void copy(DataInputView source, DataOutputView target) throws IOException {

		// read length
		int arity = source.readInt();

		// write length if necessary
		target.writeInt(arity);

		// initialize or resize nullFields if necessary
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		// read and copy null mask
		NullMaskSerDeUtils.readAndCopyNullMask(nullFields, arity, source, target);

		// copy non-null fields
		for (int i = 0; i < arity; i++) {
			if(!this.nullFields[i]) {
				fieldSer.copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof UnknownTupleSerializer) {
			UnknownTupleSerializer other = (UnknownTupleSerializer) obj;

			return other.canEqual(this) &&
					fieldSer == other.fieldSer;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return this.fieldSer.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof UnknownTupleSerializer;
	}

	private Tuple getReuseOrNew(Tuple reuse, int arity) {

		if(reuse.isUnmodifiable() || reuse.size() != arity) {
			return Tuple.size(arity);
		}
		else {
			return reuse;
		}
	}
}
