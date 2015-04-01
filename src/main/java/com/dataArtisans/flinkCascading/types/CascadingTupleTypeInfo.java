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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.Comparator;

public class CascadingTupleTypeInfo extends GenericTypeInfo<Tuple> {

	private final Comparator[] comparators;

	public CascadingTupleTypeInfo() {
		this(null);
	}

	public CascadingTupleTypeInfo(Comparator[] comparators) {
		super(Tuple.class);
		this.comparators = comparators;
	}

	@Override
	public TypeSerializer<Tuple> createSerializer(ExecutionConfig config) {
		return new CascadingTupleSerializer(config);
	}

	@Override
	public TypeComparator<Tuple> createComparator(boolean sortOrderAscending, ExecutionConfig config) {
		if(comparators == null) {
			return new CascadingTupleComparator(sortOrderAscending, this.createSerializer(config));
		}
		else {
			return new CascadingTupleComparator(sortOrderAscending, comparators, this.createSerializer(config));
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == CascadingTupleTypeInfo.class) {
			return true;
		} else {
			return false;
		}
	}

}
