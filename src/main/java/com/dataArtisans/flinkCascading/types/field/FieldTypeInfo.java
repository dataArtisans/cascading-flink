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

package com.dataArtisans.flinkCascading.types.field;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import java.util.Comparator;

/**
 * Cascading field type info. Uses a generic (Kryo) serializer.
 *
 */
public class FieldTypeInfo extends TypeInformation<Comparable> implements AtomicType {

	private Comparator<Comparable> fieldComparator = null;

	private TypeInformation<Comparable> fieldTypeInfo;

	public FieldTypeInfo() {

	}

	public FieldTypeInfo(TypeInformation<Comparable> fieldTypeInfo) {
		if(fieldTypeInfo.isBasicType())
		this.fieldTypeInfo = fieldTypeInfo;
	}

	public void setCustomComparator(Comparator<Comparable> comparator) {
		this.fieldTypeInfo = null;
		this.fieldComparator = comparator;
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
	public Class getTypeClass() {
		if(fieldTypeInfo != null) {
			return this.fieldTypeInfo.getTypeClass();
		}
		else {
			return Comparable.class;
		}
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<Comparable> createSerializer(ExecutionConfig config) {
		if(fieldTypeInfo != null) {
			return this.fieldTypeInfo.createSerializer(config);
		}
		else {
			return new KryoSerializer(Comparable.class, config);
		}
	}

	@Override
	public TypeComparator<Comparable> createComparator(boolean sortOrderAscending, ExecutionConfig config) {

		TypeSerializer<Comparable> serializer = this.createSerializer(config);

		if(this.fieldTypeInfo != null) {
			TypeComparator<Comparable> fieldComparator = ((AtomicType)fieldTypeInfo).createComparator(sortOrderAscending, config);
			return new WrappingFieldComparator(fieldComparator, sortOrderAscending, serializer, Comparable.class);
		}
		else {
			if (this.fieldComparator == null) {
				return new FieldComparator(sortOrderAscending, serializer, Comparable.class);
			} else {
				return new CustomFieldComparator(sortOrderAscending, this.fieldComparator, this.createSerializer(config));
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		return  (o instanceof FieldTypeInfo);
	}
}
