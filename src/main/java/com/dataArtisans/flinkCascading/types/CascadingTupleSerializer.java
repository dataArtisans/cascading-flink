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

package com.dataArtisans.flinkCascading.types;

import cascading.tuple.Tuple;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.NullFieldException;

import java.io.IOException;

public class CascadingTupleSerializer extends TupleSerializerBase<Tuple> {

	private static final long serialVersionUID = 1L;

	public CascadingTupleSerializer(TypeSerializer<?>[] fieldSerializers) {
		super(Tuple.class, fieldSerializers);
	}

	@Override
	public CascadingTupleSerializer duplicate() {
		boolean stateful = false;
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer<?>[fieldSerializers.length];

		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
			if (duplicateFieldSerializers[i] != fieldSerializers[i]) {
				// at least one of them is stateful
				stateful = true;
			}
		}

		if (stateful) {
			return new CascadingTupleSerializer(duplicateFieldSerializers);
		} else {
			return this;
		}
	}

	@Override
	public Tuple createInstance() {
		try {
			Tuple t = Tuple.size(this.fieldSerializers.length);

			for (int i = 0; i < arity; i++) {
				t.set(i, fieldSerializers[i].createInstance());
			}

			return t;
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public Tuple createInstance(Object[] fields) {

		try {
			Tuple t = Tuple.size(this.fieldSerializers.length);

			for (int i = 0; i < arity; i++) {
				t.set(i, fields[i]);
			}

			return t;
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public Tuple copy(Tuple from) {
		Tuple target = Tuple.size(this.fieldSerializers.length);
		for (int i = 0; i < arity; i++) {
			Object copy = fieldSerializers[i].copy(from.getObject(i));
			target.set(i, copy);
		}
		return target;
	}

	@Override
	public Tuple copy(Tuple from, Tuple reuse) {
		for (int i = 0; i < arity; i++) {
			Object copy = fieldSerializers[i].copy(from.getObject(i), reuse.getObject(i));
			reuse.set(i, copy);
		}

		return reuse;
	}

	@Override
	public void serialize(Tuple value, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object o = value.getObject(i);
			try {
				fieldSerializers[i].serialize(o, target);
			} catch (NullPointerException npex) {
				throw new NullFieldException(i);
			}
		}
	}

	@Override
	public Tuple deserialize(DataInputView source) throws IOException {
		Tuple tuple = Tuple.size(this.fieldSerializers.length);
		for (int i = 0; i < arity; i++) {
			Object field = fieldSerializers[i].deserialize(source);
			tuple.set(i, field);
		}
		return tuple;
	}

	@Override
	public Tuple deserialize(Tuple reuse, DataInputView source) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object field = fieldSerializers[i].deserialize(reuse.getObject(i), source);
			reuse.set(i, field);
		}
		return reuse;
	}
}
