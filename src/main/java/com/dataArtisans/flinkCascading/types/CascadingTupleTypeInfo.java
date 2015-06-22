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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CascadingTupleTypeInfo extends CompositeType<Tuple> {

	private final int numFields;
	private final String[] fieldNames;
	private final Map<String, Integer> fieldNameIndex;
	private final TypeInformation[] fieldTypesInfos;

	private TypeComparator<?>[] fieldComparators;
	private int[] logicalKeyFields;
	private int comparatorHelperIndex;

	public CascadingTupleTypeInfo() {
		// TODO: REMOVE
		super(null);
		throw new UnsupportedOperationException();
	}

	public CascadingTupleTypeInfo(Comparator[] x) {
		// TODO: REMOVE
		super(null);
		throw new UnsupportedOperationException();
	}

	public CascadingTupleTypeInfo(Fields fields) {
		super(Tuple.class);
		this. fieldTypesInfos = computeFieldTypes(fields);

		this.numFields = fields.size();
		this.fieldNames = new String[numFields];
		this.fieldNameIndex = new HashMap<String, Integer>(numFields);
		for(int i=0; i<numFields; i++) {
			this.fieldNames[i] = fields.get(i).toString();
			this.fieldNameIndex.put(this.fieldNames[i], i);
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
		return numFields;
	}

	@Override
	public int getTotalFields() {
		return numFields;
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
		if(!this.fieldNameIndex.containsKey(fieldName)) {
			throw new InvalidArgumentException("\""+fieldName+"\" not a field of this tuple type");
		}
		return this.fieldNameIndex.get(fieldName);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldName) {
		if(!this.fieldNameIndex.containsKey(fieldName)) {
			throw new InvalidArgumentException("\""+fieldName+"\" not a field of this tuple type");
		}
		int idx = this.fieldNameIndex.get(fieldName);
		return fieldTypesInfos[idx];
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(int fieldIdx) {
		return fieldTypesInfos[fieldIdx];
	}

	@Override
	public void getFlatFields(String expressionKey, int offset, List<FlatFieldDescriptor> list) {

		int fieldIdx = this.getFieldIndex(expressionKey);
		list.add(new FlatFieldDescriptor(offset+fieldIdx, this.fieldTypesInfos[fieldIdx]));
	}

	@Override
	public TypeSerializer<Tuple> createSerializer(ExecutionConfig config) {

		TypeSerializer<?>[] fieldSerializers = new TypeSerializer[this.numFields];
		for(int i=0; i<numFields; i++) {
			fieldSerializers[i] = this.fieldTypesInfos[i].createSerializer(config);
		}

		return new CascadingTupleSerializer(fieldSerializers);
	}

	protected void initializeNewComparator(int localKeyCount) {
		this.fieldComparators = new TypeComparator[localKeyCount];
		this.logicalKeyFields = new int[localKeyCount];
		this.comparatorHelperIndex = 0;
	}

	protected void addCompareField(int fieldId, TypeComparator<?> comparator) {
		this.fieldComparators[this.comparatorHelperIndex] = comparator;
		this.logicalKeyFields[this.comparatorHelperIndex] = fieldId;
		this.comparatorHelperIndex++;
	}

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
			serializers[i] = this.fieldTypesInfos[i].createSerializer(executionConfig);
		}

		if(finalFieldComparators.length != 0 && finalLogicalKeyFields.length != 0 && serializers.length != 0 && finalFieldComparators.length == finalLogicalKeyFields.length) {
			return new CascadingTupleComparator(finalLogicalKeyFields, finalFieldComparators, serializers);
//			throw new UnsupportedOperationException("Not yet supported!"); // TODO
		} else {
			throw new IllegalArgumentException("Tuple comparator creation has a bug");
		}
	}

	private static TypeInformation[] computeFieldTypes(Fields fields) {

		// TODO: check for special comparators in fields and use them if present

		int numFields = fields.size();
		TypeInformation[] fieldTypes = new TypeInformation[numFields];
		for(int i=0; i<numFields; i++) {
			// TODO: check for special types
			Class fieldClazz = fields.getTypeClass(i);
			if (fieldClazz == null) {
				// TODO: check
				fieldClazz = Comparable.class;
			}
			fieldTypes[i] = new GenericTypeInfo(fieldClazz);
		}

		return fieldTypes;
	}

}
