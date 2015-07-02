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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.types.field.FieldTypeInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TupleTypeInfo extends CompositeType<Tuple> {

	private Fields fields;

	private int minLength;
	private String[] fieldNames;
	private FieldTypeInfo[] fieldTypes;

	private int[] keyIdxs = new int[0];

	private TypeComparator<?>[] fieldComparators;
	private int[] logicalKeyFields;
	private int comparatorHelperIndex;

	public TupleTypeInfo(Fields fields) {
		super(Tuple.class);

		this.fields = fields;

		if(fields.isDefined()) {
			this.minLength = fields.size();
			this.fieldNames = new String[minLength];
			this.fieldTypes = new FieldTypeInfo[minLength];

			Comparator[] comps = fields.getComparators();

			for(int i=0; i<minLength; i++) {
				this.fieldNames[i] = Integer.toString(i);
				this.fieldTypes[i] = new FieldTypeInfo();
				if(comps != null && comps.length > i && comps[i] != null) {
					this.fieldTypes[i].setCustomComparator(comps[i]);
				}
			}
		}
		else {
			if(fields.isUnknown()) {
				this.minLength = 1;
				this.fieldNames = new String[]{"0"};
				this.fieldTypes = new FieldTypeInfo[] {new FieldTypeInfo()};
			}
			else {
				throw new IllegalArgumentException("Unsupported Fields: "+fields);
			}
		}
	}

//	public String[] registerKeyFields(Fields keyFields) {
//
//		String[] serdePos = new String[keyFields.size()];
//
//		int[] keyPos = this.fields.getPos(keyFields);
//		Comparator[] comps = keyFields.getComparators();
//
//		int oldLen = this.keyIdxs.length;
//		this.keyIdxs = Arrays.copyOf(this.keyIdxs, oldLen + keyPos.length);
//		for(int j=0; j<keyPos.length; j++) {
//			// update min length
//			updateMinLength(keyPos[j] + 1);
//
//			// set serde position
//			this.keyIdxs[oldLen + j] = keyPos[j];
//			serdePos[j] = Integer.toString(oldLen + j);
//
//			// set custom comparator (if any)
//			if(comps[j] != null) {
//				// set custom comparator
//				this.fieldTypes[oldLen + j].setCustomComparator(comps[j]);
//			}
//		}
//
//		return serdePos;
//	}

	public String[] registerKeyFields(Fields keyFields) {

		String[] serdePos = new String[keyFields.size()];

		int[] keyPos = this.fields.getPos(keyFields);
		Comparator[] comps = keyFields.getComparators();

		for(int j=0; j<keyPos.length; j++) {
			// update min length
			updateMinLength(keyPos[j] + 1);

			// set serde position
			serdePos[j] = Integer.toString(keyPos[j]);

			// set custom comparator (if any)
			if(comps[j] != null) {
				// set custom comparator
				this.fieldTypes[keyPos[j]].setCustomComparator(comps[j]);
			}
		}

		return serdePos;
	}

	public Fields getFields() {
		return this.fields;
	}

	private void updateMinLength(int newMinLength) {
		if(newMinLength > this.minLength) {

			// update known field names & field types
			this.fieldNames = Arrays.copyOf(this.fieldNames, newMinLength);
			this.fieldTypes = Arrays.copyOf(this.fieldTypes, newMinLength);
			for(int i=minLength; i<newMinLength; i++) {
				fieldNames[i] = Integer.toString(i);
				fieldTypes[i] = new FieldTypeInfo();
			}

			// update maxIdx
			this.minLength = newMinLength;
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
		return this.minLength;
	}

	@Override
	public int getTotalFields() {
		return this.minLength;
	}

	@Override
	public Class<Tuple> getTypeClass() {
		return Tuple.class;
	}

	@Override
	public String[] getFieldNames() {
		return this.fieldNames;
	}

	@Override
	public int getFieldIndex(String fieldName) {

		try {
			int idx = Integer.parseInt(fieldName);
			if(idx < this.fieldTypes.length && this.fieldTypes[idx] != null) {
				return idx;
			}
			else {
				throw new IndexOutOfBoundsException("Field index out of bounds.");
			}
		}
		catch(NumberFormatException nfe) {
			throw new IllegalArgumentException("Only numeric keys supported for Cascading tuples");
		}
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		int idx = getFieldIndex(fieldExpression);
		return this.getTypeAt(idx);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(int idx) {
		if(idx < this.fieldTypes.length && this.fieldTypes[idx] != null) {
			return (TypeInformation<X>) this.fieldTypes[idx];
		}
		else {
			throw new IndexOutOfBoundsException("Field index out of bounds.");
		}
	}

	@Override
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> list) {

		if(fieldExpression.equals("*")) {
			for(int i=0; i<this.minLength; i++) {
				list.add(new FlatFieldDescriptor(offset+i, getTypeAt(i)));
			}
		}
		else {
			int fieldIdx = this.getFieldIndex(fieldExpression);
			list.add(new FlatFieldDescriptor(offset+fieldIdx, getTypeAt(fieldIdx)));
		}
	}

	@Override
	public TypeSerializer<Tuple> createSerializer(ExecutionConfig config) {
		return new TupleSerializer(new FieldTypeInfo().createSerializer(config), this.minLength);
	}

	@Override
	protected void initializeNewComparator(int localKeyCount) {
		this.fieldComparators = new TypeComparator[localKeyCount];
		this.logicalKeyFields = new int[localKeyCount];
		this.comparatorHelperIndex = 0;
	}

	@Override
	protected void addCompareField(int fieldId, TypeComparator<?> comparator) {
		this.fieldComparators[this.comparatorHelperIndex] = comparator;
		this.logicalKeyFields[this.comparatorHelperIndex] = fieldId;
		this.comparatorHelperIndex++;
	}

	@Override
	protected TypeComparator<Tuple> getNewComparator(ExecutionConfig executionConfig) {
		TypeComparator[] finalFieldComparators = (TypeComparator[]) Arrays.copyOf(this.fieldComparators, this.comparatorHelperIndex);
		int[] finalLogicalKeyFields = Arrays.copyOf(this.logicalKeyFields, this.comparatorHelperIndex);
		int maxKey = 0;
		int[] fieldSerializers = finalLogicalKeyFields;
		int numKeyFields = finalLogicalKeyFields.length;

		for(int i = 0; i < numKeyFields; ++i) {
			int key = fieldSerializers[i];
			maxKey = Math.max(maxKey, key);
		}

		TypeSerializer[] serializers = new TypeSerializer[maxKey + 1];

		for(int i = 0; i <= maxKey; ++i) {
			serializers[i] = new FieldTypeInfo().createSerializer(executionConfig);
		}

		if(finalFieldComparators.length != 0 && finalLogicalKeyFields.length != 0 && serializers.length != 0 && finalFieldComparators.length == finalLogicalKeyFields.length) {
			return new TupleComparator(finalLogicalKeyFields, finalFieldComparators, serializers);
		} else {
			throw new IllegalArgumentException("Tuple comparator creation has a bug");
		}
	}

	@Override
	public boolean equals(Object o) {

		if (o instanceof TupleTypeInfo) {
			return Arrays.equals(this.keyIdxs, ((TupleTypeInfo)o).keyIdxs);
		}
		else {
			return false;
		}
	}
}
