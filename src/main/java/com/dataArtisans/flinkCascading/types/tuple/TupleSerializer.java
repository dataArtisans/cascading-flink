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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class TupleSerializer extends TypeSerializer<Tuple> {

	private static final long serialVersionUID = 1L;
	private boolean[] nullFields;
	private TypeSerializer fieldSer;
//	private int[] serPos;
	private int minLength;

	public TupleSerializer(TypeSerializer fieldSer, int minLength) {
		this.fieldSer = fieldSer;
		this.minLength = minLength;
	}

//	public TupleSerializer(TypeSerializer fieldSer, int[] serPos) {
//		this.fieldSer = fieldSer;
//		this.init(serPos);
//	}

//	private void init(int[] serPos) {
//
//		int max = 0;
//		for(int i=0;i<serPos.length; i++) {
//			max = (serPos[i] > max) ? serPos[i] : max;
//		}
//
//		this.minLength = max + 1;
//
//		if(max + 1 > serPos.length) {
//
//			int[] serPosExtd = Arrays.copyOf(serPos, max + 1);
//			for(int i=serPos.length; i < max+1; i++) {
//
//				// find next field to serialize
//				for(int j=0; j<max+1; j++) {
//					// check if already set
//					boolean found = false;
//					for(int k=0; k<serPos.length; k++) {
//						if(k == j) {
//							found = true;
//							break;
//						}
//					}
//					if(found) {
//						serPosExtd[i] = j;
//						break;
//					}
//				}
//			}
//			this.serPos = serPosExtd;
//		}
//		else {
//			this.serPos = serPos;
//		}
//	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TupleSerializer duplicate() {
//		return new TupleSerializer(this.fieldSer.duplicate(), serPos);
		return new TupleSerializer(this.fieldSer.duplicate(), minLength);
	}

	@Override
	public Tuple createInstance() {
		try {
			return Tuple.size(minLength);
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

		// write length
		target.writeInt(value.size());

		// write null markers TODO: use bit mask
//		for (int i = 0; i < this.serPos.length; i++) {
//			target.writeBoolean(value.getObject(this.serPos[i]) == null);
//		}
//		for (int i = this.serPos.length; i < value.size(); i++) {
//			target.writeBoolean(value.getObject(i) == null);
//		}
		for (int i = 0; i < value.size(); i++) {
			target.writeBoolean(value.getObject(i) == null);
		}

//		for (int i = 0; i < this.serPos.length; i++) {
//			Object o = value.getObject(this.serPos[i]);
//			if(o != null) {
//				fieldSer.serialize(o, target);
//			}
//		}
//		for (int i = this.serPos.length; i < value.size(); i++) {
//			Object o = value.getObject(i);
//			if(o != null) {
//				fieldSer.serialize(o, target);
//			}
//		}
		for (int i = 0; i < value.size(); i++) {
			Object o = value.getObject(i);
			if(o != null) {
				fieldSer.serialize(o, target);
			}
		}
	}

	@Override
	public Tuple deserialize(DataInputView source) throws IOException {

		int arity = source.readInt();
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		// TODO: read more efficient bit mask
		for (int i = 0; i < arity; i++) {
			this.nullFields[i] = source.readBoolean();
		}

		Tuple tuple = Tuple.size(arity);
		for (int i = 0; i < arity; i++) {
			Object field;
			if(!this.nullFields[i]) {
				field = fieldSer.deserialize(source);
			}
			else {
				field = null;
			}
//			if(i < this.serPos.length) {
//				tuple.set(this.serPos[i], field);
//			}
//			else {
//				tuple.set(i, field);
//			}
			tuple.set(i, field);
		}

		return tuple;
	}

	@Override
	public Tuple deserialize(Tuple reuse, DataInputView source) throws IOException {

		int arity = source.readInt();
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}
		if(reuse.size() != arity) {
			reuse = Tuple.size(arity);
		}

		// TODO: read more efficient bit mask
		for (int i = 0; i < arity; i++) {
			this.nullFields[i] = source.readBoolean();
		}

		for (int i = 0; i < arity; i++) {
			Object field;
			if(!this.nullFields[i]) {
				field = fieldSer.deserialize(source);
			}
			else {
				field = null;
			}
//			if(i < this.serPos.length) {
//				reuse.set(this.serPos[i], field);
//			}
//			else {
//				reuse.set(i, field);
//			}
			reuse.set(i, field);
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

		int arity = source.readInt();
		if(this.nullFields == null || this.nullFields.length < arity) {
			this.nullFields = new boolean[arity];
		}

		target.writeInt(arity);

		for (int i = 0; i < arity; i++) {
			this.nullFields[i] = source.readBoolean();
			target.writeBoolean(this.nullFields[i]);
		}

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

}
