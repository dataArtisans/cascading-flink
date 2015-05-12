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

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;

import java.io.IOException;

public class PassOnCollector extends TupleBuilderCollector {

	private Aggregator followingAgg;
	private TupleBuilderCollector followingCollector;
	private FlowProcess ffp;
	private AggregatorCall call;

	public PassOnCollector(Aggregator followingAgg, TupleBuilderCollector followingCollector, FlowProcess ffp, AggregatorCall call, TupleBuilder builder, Fields declaredFields) {
		super(builder, declaredFields);
		this.followingAgg = followingAgg;
		this.followingCollector = followingCollector;
		this.ffp = ffp;
		this.call = call;
	}

	@Override
	protected void collect(TupleEntry outTupleE) throws IOException {

		// build new tuple
		Tuple outgoing = buildTuple(outTupleE.getTuple());

		// set tuple as new in tuple in following collector
		followingCollector.setInTuple(outgoing);

		// call aggregator complete to emit result tuple into following collector
		followingAgg.complete(ffp, call);
	}
}
