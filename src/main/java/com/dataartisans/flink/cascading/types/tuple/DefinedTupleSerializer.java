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

import cascading.flow.FlowException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;

public class DefinedTupleSerializer extends TypeSerializer<Tuple> {

	private static final long serialVersionUID = 1L;

	private final Fields fields;
	private final TypeSerializer[] fieldSers;
	private final int length;
	private final boolean[] nullFields;

	public DefinedTupleSerializer(Fields fields, TypeSerializer[] fieldSers) {
		if(!fields.isDefined()) {
			throw new RuntimeException("DefinedTupleSerializer requires defined Fields schema");
		}
		if(fieldSers == null || fieldSers.length != fields.size()) {
			throw new RuntimeException("Exactly one field serializer required for each tuple field.");
		}

		this.fields = fields;
		this.fieldSers = fieldSers;
		this.length = fields.size();
		this.nullFields = new boolean[this.length];
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public DefinedTupleSerializer duplicate() {
		TypeSerializer[] copies = new TypeSerializer[this.fieldSers.length];
		for(int i=0; i<copies.length; i++) {
			copies[i] = this.fieldSers[i].duplicate();
		}
		return new DefinedTupleSerializer(this.fields, copies);
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
			try {
				Object orig = from.getObject(i);
				if (orig != null) {
					target.set(i, fieldSers[i].copy(orig));
				}
				else {
					target.set(i, null);
				}
			}
			catch(ClassCastException cce) {
				throw new FlowException("Unexpected type of field \""+fields.get(i)+"\" encountered. " +
										"Should have been "+fields.getType(i)+" but was "+from.getObject(i).getClass()+".", cce);
			}
		}
		return target;
	}

	@Override
	public Tuple copy(Tuple from, Tuple reuse) {

		Tuple tuple = getReuseOrNew(reuse);

		for (int i = 0; i < from.size(); i++) {
			try {
				Object fromOrig = from.getObject(i);
				Object reuseOrig = tuple.getObject(i);

				Object copy;
				if (fromOrig == null) {
					copy = null;
				}
				else if (reuseOrig != null) {
					copy = fieldSers[i].copy(fromOrig, reuseOrig);
				}
				else {
					copy = fieldSers[i].copy(fromOrig);
				}
				tuple.set(i, copy);
			}
			catch(ClassCastException cce) {
				throw new FlowException("Unexpected type of field \""+fields.get(i)+"\" encountered. " +
						"Should have been "+fields.getType(i)+" but was "+from.getObject(i).getClass()+".", cce);
			}
		}

		return tuple;
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
				try {
					fieldSers[i].serialize(o, target);
				}
				catch(ClassCastException cce) {
					throw new FlowException("Unexpected type of field \""+fields.get(i)+"\" encountered. " +
											"Should have been "+fields.getType(i)+" but was "+o.getClass()+".", cce);
				}
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
				field = fieldSers[i].deserialize(source);
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

		Tuple tuple = getReuseOrNew(reuse);

		// read null mask
		NullMaskSerDeUtils.readNullMask(nullFields, this.length, source);

		for (int i = 0; i < this.length; i++) {
			Object field;
			if(!this.nullFields[i]) {
				field = fieldSers[i].deserialize(source);
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

		// read and copy null mask
		NullMaskSerDeUtils.readAndCopyNullMask(nullFields, this.length, source, target);

		// copy non-null fields
		for (int i = 0; i < this.length; i++) {
			if(!this.nullFields[i]) {
				fieldSers[i].copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DefinedTupleSerializer) {
			DefinedTupleSerializer other = (DefinedTupleSerializer) obj;

			return other.canEqual(this) &&
					fields.equals(other.fields) &&
					Arrays.equals(this.fieldSers, other.fieldSers);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * this.fields.hashCode() + Arrays.hashCode(this.fieldSers);
	}

	@Override
	public TypeSerializerSnapshot<Tuple> snapshotConfiguration() {
		return null;
	}

	public boolean canEqual(Object obj) {
		return obj instanceof DefinedTupleSerializer;
	}

	private Tuple getReuseOrNew(Tuple reuse) {
		if(reuse.isUnmodifiable()) {
			return Tuple.size(this.length);
		}
		else {
			return reuse;
		}
	}

}
