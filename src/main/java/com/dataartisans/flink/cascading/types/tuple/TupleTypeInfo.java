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
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class TupleTypeInfo extends CompositeType<Tuple> {

	private final static int NEG_FIELD_POS_OFFSET = Integer.MAX_VALUE / 2;

	private Fields schema;

	private final int length;
	private LinkedHashMap<String, FieldTypeInfo> fieldTypes;
	private HashMap<String, Integer> fieldIndexes;

	public TupleTypeInfo(Fields schema) {
		super(Tuple.class);

		this.schema = schema;
		this.fieldIndexes = new HashMap<String, Integer>();

		if(schema.isDefined()) {
			this.length = schema.size();
			this.fieldTypes = new LinkedHashMap<String, FieldTypeInfo>(this.length);

			Comparator[] comps = schema.getComparators();
			Class[] typeClasses = schema.getTypesClasses();

			for(int i=0; i<length; i++) {
				String fieldName = getFieldName(i);
				FieldTypeInfo fieldType = getFieldTypeInfo(i, typeClasses, comps);

				this.fieldTypes.put(fieldName, fieldType);
				this.fieldIndexes.put(fieldName, i);
			}
		}
		else {
			if(schema.isUnknown()) {
				this.length = -1;
				this.fieldTypes = new LinkedHashMap<String, FieldTypeInfo>(16);

				this.fieldTypes.put("0", new FieldTypeInfo());
				this.fieldIndexes.put("0", 0);
			}
			else {
				throw new IllegalArgumentException("Unsupported Fields: "+schema);
			}
		}
	}

	public String[] registerKeyFields(Fields keyFields) {

		int[] keyPos;
		if(keyFields.isAll()) {
			keyPos = new int[this.schema.size()];
			for(int i=0; i<keyPos.length; i++) {
				keyPos[i] = i;
			}
		}
		else {
			keyPos = this.schema.getPos(keyFields);
		}

		String[] serdePos = new String[keyPos.length];

		Comparator[] keyComps = keyFields.getComparators();
		Class[] keyTypes = keyFields.getTypesClasses();

		for(int j=0; j<keyPos.length; j++) {

			String fieldName = getFieldName(keyPos[j]);

			if(!this.fieldIndexes.containsKey(fieldName)) {
				this.fieldIndexes.put(fieldName, keyPos[j]);
			}

			FieldTypeInfo fieldType = this.fieldTypes.get(fieldName);
			if(fieldType == null) {
				fieldType = new FieldTypeInfo();
				this.fieldTypes.put(fieldName, fieldType);
			}

			// set serde position
			serdePos[j] = fieldName;

			if(keyTypes != null && keyTypes.length > j && keyTypes[j] != null) {
				// set key type
				fieldType.setFieldType(keyTypes[j]);
			}

			if(keyComps != null && keyComps.length > j && keyComps[j] != null) {
				// set custom key comparator
				fieldType.setCustomComparator(keyComps[j]);
			}
		}

		return serdePos;
	}

	public Fields getSchema() {
		return this.schema;
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
		return (String[])this.fieldTypes.keySet().toArray();
	}

	@Override
	public int getFieldIndex(String fieldName) {

		try {
			int idx = this.fieldIndexes.get(fieldName);
			if(this.fieldTypes.get(fieldName) != null) {
				return getFlinkPos(idx);
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

		idx = getCascadingPos(idx);

		String fieldName = getFieldName(idx);
		if(this.fieldTypes.get(fieldName) != null) {
			return (TypeInformation<X>) this.fieldTypes.get(fieldName);
		}
		else {
			throw new IndexOutOfBoundsException("Field index out of bounds.");
		}
	}

	@Override
	protected TypeComparatorBuilder<Tuple> createTypeComparatorBuilder() {
		// not required
		throw new UnsupportedOperationException();
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

		if(this.length > 0) {
			// create serializer for tuple with schema

			TypeSerializer[] fieldSers;
			fieldSers = new TypeSerializer[this.length];
			for(String field : this.fieldTypes.keySet()) {
				Integer fieldIdx = Integer.parseInt(field);
				fieldSers[fieldIdx] = this.fieldTypes.get(field).createSerializer(config);
			}
			return new DefinedTupleSerializer(this.schema, fieldSers);
		}
		else {
			// create serializer for tuple without schema

			TypeSerializer defaultFieldSer = new FieldTypeInfo().createSerializer(config);
			return new UnknownTupleSerializer(defaultFieldSer);
		}
	}

	@Override
	public TypeComparator<Tuple> createComparator(int[] keyIdxs, boolean[] orders, int offset, ExecutionConfig config) {

		if(keyIdxs.length == 0) {
			throw new RuntimeException("Empty key indexes");
		}
		if(offset != 0) {
			throw new RuntimeException("Only 0 offset supported.");
		}

		// get key comparators
		TypeComparator<?>[] keyComps = new TypeComparator[keyIdxs.length];
		for(int i = 0; i < keyIdxs.length; i++) {
			keyComps[i] = ((AtomicType)this.getTypeAt(keyIdxs[i])).createComparator(orders[i], config);
		}

		if(length > 0) {
			// comparator for tuples with defined schema
			int maxKey = 0;
			for(int i = 0; i < keyIdxs.length; ++i) {
				int key = keyIdxs[i];
				maxKey = Math.max(maxKey, key);
			}

			// get field serializers up to max key
			TypeSerializer[] serializers = new TypeSerializer[maxKey + 1];
			for(int i = 0; i <= maxKey; ++i) {
				serializers[i] = this.fieldTypes.get(Integer.toString(i)).createSerializer(config);
			}

			return new DefinedTupleComparator(keyIdxs, keyComps, serializers, this.length);
		}
		else {
			// comparator for unknown tuples
			int[] cascadingKeyIdx = new int[keyIdxs.length];
			for(int i=0; i<cascadingKeyIdx.length; i++) {
				cascadingKeyIdx[i] = getCascadingPos(keyIdxs[i]);
			}

			return new UnknownTupleComparator(cascadingKeyIdx, keyComps, new FieldTypeInfo().createSerializer(config));
		}

	}

	@Override
	public boolean equals(Object o) {

		if (o instanceof TupleTypeInfo) {
			return this.schema.equalsFields(((TupleTypeInfo) o).getSchema());
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

	// this is an ugly hack to support negative relative positions on unknown schemas.
	private int getFlinkPos(int cascadingPos) {
		if(cascadingPos >= 0) {
			if(cascadingPos > NEG_FIELD_POS_OFFSET) {
				throw new RuntimeException("Maximum key position "+NEG_FIELD_POS_OFFSET+" exceeded");
			}

			return cascadingPos;
		}
		else {
			return NEG_FIELD_POS_OFFSET + (-1 * cascadingPos);
		}
	}

	// this is an ugly hack to support negative relative positions on unknown schemas.
	private int getCascadingPos(int flinkPos) {
		if(flinkPos > NEG_FIELD_POS_OFFSET) {
			return (flinkPos - NEG_FIELD_POS_OFFSET) * -1;
		}
		else {
			return flinkPos;
		}
	}

	private String getFieldName(int index) {
		if(index >= 0) {
			return Integer.toString(index);
		}
		else {
			return "neg"+(-1*index);
		}
	}
}
