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

package com.dataArtisans.flinkCascading.runtime.source;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementStage;
import cascading.flow.stream.graph.StreamGraph;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SingleOutBoundaryStage extends ElementStage<TupleEntry, Void> {

	private Tuple nextTuple;

	public SingleOutBoundaryStage(FlowProcess flowProcess, FlowElement flowElement) {
		super(flowProcess, flowElement);
		this.nextTuple = null;
	}

	@Override
	public void receive(Duct prev, TupleEntry entry) {
		
		if(this.nextTuple == null) {
			this.nextTuple = entry.getTuple();
		}
		else {
			throw new RuntimeException("Previous tuple was not fetched!");
		}
	}

	public boolean hasNextTuple() {
		return nextTuple != null;
	}

	public Tuple fetchNextTuple() {
		Tuple nextTuple = this.nextTuple;
		this.nextTuple = null;
		return nextTuple;
	}

	@Override
	public void bind( StreamGraph streamGraph ) {
		// don't do anything
	}

	@Override
	public void start( Duct previous ) {
		// don't do anything
	}

	@Override
	public void complete(Duct previous) {
		// don't do anything
	}

}
