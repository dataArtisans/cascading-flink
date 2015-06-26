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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Comparator;

/**
 * Wraps a TypeInformation and can inject a custom field comparator
 *
 * @param <T>
 */
public class CascadingFieldTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

	private TypeInformation wrappedTypeInfo;
	private Comparator<T> fieldComparator = null;

	public CascadingFieldTypeInfo(TypeInformation<T> wrappedTypeInfo) {
		this.wrappedTypeInfo = wrappedTypeInfo;
	}

	public void setCustomComparator(Comparator<T> comparator) {
		this.fieldComparator = comparator;
	}

	@Override
	public boolean isBasicType() {
		return this.wrappedTypeInfo.isBasicType();
	}

	@Override
	public boolean isTupleType() {
		return this.wrappedTypeInfo.isTupleType();
	}

	@Override
	public int getArity() {
		return this.wrappedTypeInfo.getArity();
	}

	@Override
	public int getTotalFields() {
		return this.wrappedTypeInfo.getTotalFields();
	}

	@Override
	public Class<T> getTypeClass() {
		return this.wrappedTypeInfo.getTypeClass();
	}

	@Override
	public boolean isKeyType() {
		return this.wrappedTypeInfo.isKeyType();
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return this.wrappedTypeInfo.createSerializer(config);
	}

	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig config) {

		if(this.fieldComparator == null) {
			if(this.wrappedTypeInfo instanceof AtomicType) {
				return ((AtomicType) this.wrappedTypeInfo).createComparator(sortOrderAscending, config);
			}
			else {
				throw new RuntimeException("Unable to create comparator for non-atomic wrapped type");
			}
		}
		else {
			return new CustomCascadingFieldComparator<T>(true, this.fieldComparator, this.createSerializer(config));
		}

	}
}
