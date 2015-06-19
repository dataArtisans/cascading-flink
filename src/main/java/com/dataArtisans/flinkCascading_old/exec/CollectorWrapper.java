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

package com.dataArtisans.flinkCascading_old.exec;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class CollectorWrapper extends TupleEntryCollector {

	Collector<Tuple> flinkCollector;

	public CollectorWrapper(Collector<Tuple> flinkCollector) {
		this.flinkCollector = flinkCollector;
	}

	@Override
	protected void collect(TupleEntry tupleEntry) throws IOException {
		this.flinkCollector.collect(tupleEntry.getTuple());
	}
}
