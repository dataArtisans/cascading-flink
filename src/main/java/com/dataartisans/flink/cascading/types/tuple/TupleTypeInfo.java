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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.types.field.FieldTypeInfo;
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

	private final int length;
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
			this.length = fields.size();
			this.fieldNames = new String[length];
			this.fieldTypes = new FieldTypeInfo[length];

			Comparator[] comps = fields.getComparators();
			Class[] typeClasses = fields.getTypesClasses();

			for(int i=0; i<length; i++) {
				this.fieldNames[i] = Integer.toString(i);
				this.fieldTypes[i] = getFieldTypeInfo(i, typeClasses, comps);
			}
		}
		else {
			if(fields.isUnknown()) {
				this.length = -1;
				this.fieldNames = new String[]{"0"};
				this.fieldTypes = new FieldTypeInfo[] {new FieldTypeInfo()};
			}
			else {
				throw new IllegalArgumentException("Unsupported Fields: "+fields);
			}
		}
	}

	public String[] registerKeyFields(Fields keyFields) {

		int[] keyPos;
		if(keyFields.isAll()) {
			keyPos = new int[this.fields.size()];
			for(int i=0; i<keyPos.length; i++) {
				keyPos[i] = i;
			}
		}
		else {
			keyPos = this.fields.getPos(keyFields);
		}

		String[] serdePos = new String[keyPos.length];

		Comparator[] keyComps = keyFields.getComparators();
		Class[] keyTypes = keyFields.getTypesClasses();

		for(int j=0; j<keyPos.length; j++) {
			// update min length
			extendFieldInfoArrays(keyPos[j] + 1);

			// set serde position
			serdePos[j] = Integer.toString(keyPos[j]);

			if(keyTypes != null && keyTypes.length > j && keyTypes[j] != null) {
				// set key type
				this.fieldTypes[keyPos[j]].setFieldType(keyTypes[j]);
			}

			if(keyComps != null && keyComps.length > j && keyComps[j] != null) {
				// set custom key comparator
				this.fieldTypes[keyPos[j]].setCustomComparator(keyComps[j]);
			}
		}

		return serdePos;
	}

	public Fields getFields() {
		return this.fields;
	}

	private void extendFieldInfoArrays(int newLength) {
		int curLength = this.fieldNames.length;

		if(newLength > curLength) {

			// update known field names & field types
			this.fieldNames = Arrays.copyOf(this.fieldNames, newLength);
			this.fieldTypes = Arrays.copyOf(this.fieldTypes, newLength);
			for(int i=curLength; i<newLength; i++) {
				fieldNames[i] = Integer.toString(i);
				fieldTypes[i] = new FieldTypeInfo();
			}
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
		if(this.length > 0) {
			return this.length;
		}
		else {
			return 1;
		}
	}

	@Override
	public int getTotalFields() {
		return this.getArity();
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
			for(int i=0; i<this.length; i++) {
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
		return new TupleSerializer(new FieldTypeInfo().createSerializer(config), this.length);
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
			return new TupleComparator(finalLogicalKeyFields, finalFieldComparators, serializers, this.length);
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

	private FieldTypeInfo getFieldTypeInfo(int pos, Class[] typeClasses, Comparator[] fieldComparators) {
		if(typeClasses != null && pos >= typeClasses.length) {
			throw new ArrayIndexOutOfBoundsException("Fields position out of bounds");
		}
		if(fieldComparators != null && pos >= fieldComparators.length) {
			throw new ArrayIndexOutOfBoundsException("Fields position out of bounds");
		}

		FieldTypeInfo fieldTypeInfo;
		if(typeClasses != null && typeClasses[pos] != null) {
			fieldTypeInfo = new FieldTypeInfo(typeClasses[pos]);
		}
		else {
			fieldTypeInfo = new FieldTypeInfo();
		}

		// set custom field comparator if any
		if(fieldComparators != null && fieldComparators[pos] != null) {
			fieldTypeInfo.setCustomComparator(fieldComparators[pos]);
		}

		return fieldTypeInfo;
	}
}
