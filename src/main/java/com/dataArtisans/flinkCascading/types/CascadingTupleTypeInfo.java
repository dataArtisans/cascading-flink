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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;

import java.util.Arrays;
import java.util.Comparator;

public class CascadingTupleTypeInfo extends TupleTypeInfoBase<Tuple> {

	private final int numFields;
	private final String[] fieldNames;

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
		super(Tuple.class, computeFieldTypes(fields));

		this.numFields = fields.size();
		this.fieldNames = new String[numFields];
		for(int i=0; i<numFields; i++) {
			// TODO check if we can get names for fields (field names are Comparables not Strings)
			this.fieldNames[i] = "f"+i;
		}
	}

	@Override
	public String[] getFieldNames() {
		return this.fieldNames;
	}

	@Override
	public int getFieldIndex(String s) {
		if(!s.startsWith("f")) {
			throw new IllegalArgumentException("Field names start with \"f\"");
		}
		int fieldIndex = Integer.parseInt(s.substring(1));
		return fieldIndex;
	}

	@Override
	public TypeSerializer<Tuple> createSerializer(ExecutionConfig config) {
		return new CascadingTupleSerializer(config);
	}

	protected void initializeNewComparator(int localKeyCount) {
		this.fieldComparators = new TypeComparator[localKeyCount];
		this.logicalKeyFields = new int[localKeyCount];
		this.comparatorHelperIndex = 0;
	}

	protected void addCompareField(int fieldId, TypeComparator<?> comparator) {
		this.fieldComparators[this.comparatorHelperIndex] = comparator;
		this.logicalKeyFields[this.comparatorHelperIndex] = fieldId;
		++this.comparatorHelperIndex;
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
			serializers[i] = this.types[i].createSerializer(executionConfig);
		}

		if(finalFieldComparators.length != 0 && finalLogicalKeyFields.length != 0 && serializers.length != 0 && finalFieldComparators.length == finalLogicalKeyFields.length) {
			return new TupleComparator(finalLogicalKeyFields, finalFieldComparators, serializers);
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
				fieldClazz = Object.class;
			}
			fieldTypes[i] = new GenericTypeInfo(fieldClazz);
		}

		return fieldTypes;
	}


}
