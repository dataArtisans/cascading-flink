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

package com.dataartisans.flink.cascading.runtime.hashJoin;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementStage;
import cascading.flow.stream.element.InputSource;
import cascading.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinBoundaryInStage extends ElementStage<Void, Tuple2<Tuple, Tuple[]>> implements InputSource {

	public JoinBoundaryInStage(FlowProcess flowProcess, FlowElement flowElement) {
		super(flowProcess, flowElement);
	}

	@Override
	public void receive(Duct previous, Void v) {
		throw new UnsupportedOperationException( "use run() instead" );
	}

	@Override
	public void complete(Duct previous) {

		if( next != null ) {
			super.complete(previous);
		}
	}

	@Override
	public void start(Duct previous) {
		next.start(this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run(Object input) throws Throwable {

		Tuple2<Tuple, Tuple[]> joinInputTuples;
		try {
			joinInputTuples = (Tuple2<Tuple, Tuple[]>)input;
		}
		catch(ClassCastException cce) {
			throw new RuntimeException("JoinBoundaryInStage expects Tuple2<Tuple, Tuple[]>", cce);
		}

		flowProcess.increment( StepCounters.Tuples_Read, 1 );
		flowProcess.increment( SliceCounters.Tuples_Read, 1 );

		next.receive(this, joinInputTuples);
	}
}
