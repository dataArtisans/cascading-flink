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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.types.tuple.TupleTypeInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;

public class TupleArrayTypeInfo extends TypeInformation<Tuple[]> {

	private final int length;
	private final int fillLength;
	private final TupleTypeInfo[] tupleTypes;

	public TupleArrayTypeInfo(int length, Fields[] fields) {
		this.length = length;
		this.fillLength = fields.length;

		this.tupleTypes = new TupleTypeInfo[this.fillLength];
		for(int i=0; i<this.fillLength; i++) {
			this.tupleTypes[i] = new TupleTypeInfo(fields[i]);
		}
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<Tuple[]> getTypeClass() {
		return Tuple[].class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<Tuple[]> createSerializer(ExecutionConfig executionConfig) {

		TypeSerializer<Tuple>[] tupleSerializers = new TypeSerializer[this.fillLength];
		for(int i=0; i<this.fillLength; i++) {
			tupleSerializers[i] = this.tupleTypes[i].createSerializer(executionConfig);
		}

		return new TupleArraySerializer(this.length, tupleSerializers);
	}

	@Override
	public String toString() {
		return "Tuple["+this.length+"] "+ Arrays.toString(tupleTypes);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleArrayTypeInfo) {
			TupleArrayTypeInfo other = (TupleArrayTypeInfo) obj;

			// sloppy equals check, Cascading does not always provide types.
			// We rely on Cascading to do the type checking here.
			return other.canEqual(this) &&
					this.length == other.length &&
					Arrays.equals(this.tupleTypes, other.tupleTypes);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * this.length + Arrays.hashCode(this.tupleTypes);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleArrayTypeInfo;
	}
}
