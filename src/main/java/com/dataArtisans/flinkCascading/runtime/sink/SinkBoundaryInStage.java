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

package com.dataArtisans.flinkCascading.runtime.sink;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
import cascading.flow.planner.Scope;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.ElementStage;
import cascading.flow.stream.element.InputSource;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SinkBoundaryInStage extends ElementStage<Void, TupleEntry> implements InputSource {

	private boolean nextStarted;
	private TupleEntry tupleEntry;

	public SinkBoundaryInStage(FlowProcess flowProcess, FlowElement flowElement, FlowNode node) {
		super(flowProcess, flowElement);
		this.nextStarted = false;

		Scope inScope = node.getElementGraph().incomingEdgesOf(flowElement).iterator().next();

		Fields inFields;
		if(inScope.isEvery()) {
			inFields = inScope.getOutGroupingFields();
		}
		else {
			inFields = inScope.getOutValuesFields();
		}

		this.tupleEntry = new TupleEntry(inFields);
	}

	@Override
	public void receive(Duct previous, Void v) {
		throw new UnsupportedOperationException( "use run() instead" );
	}

	@Override
	public void run(Object input) throws Throwable {

		if(!this.nextStarted) {
			next.start(this);
			this.nextStarted = true;
		}

		try {
			Tuple tuple = (Tuple)input;
			tupleEntry.setTuple(tuple);
			flowProcess.increment( StepCounters.Tuples_Read, 1 );
			flowProcess.increment(SliceCounters.Tuples_Read, 1);
		}
		catch( OutOfMemoryError error ) {
			handleReThrowableException("out of memory, try increasing task memory allocation", error);
		}
		catch( CascadingException exception ) {
			handleException(exception, null);
		}
		catch( Throwable throwable ) {
			handleException(new DuctException("internal error", throwable), null);
		}

		next.receive( this, tupleEntry );

	}
}
