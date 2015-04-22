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

package com.dataArtisans.flinkCascading.exec;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class FlinkCollector extends TupleBuilderCollector {

	private Collector<Tuple> wrappedCollector;

	public FlinkCollector(TupleBuilder builder, Fields declaredFields) {
		super(builder, declaredFields);
	}

	public FlinkCollector(Collector<Tuple> wrappedCollector, TupleBuilder builder, Fields declaredFields) {
		super(builder, declaredFields);
		this.wrappedCollector = wrappedCollector;
	}

	public void setWrappedCollector(Collector<Tuple> wrappedCollector) {
		this.wrappedCollector = wrappedCollector;
	}

	@Override
	protected void collect(TupleEntry outTupleE) throws IOException {

		Tuple outgoing = buildTuple(outTupleE.getTuple());
		this.wrappedCollector.collect(outgoing);
	}
}
