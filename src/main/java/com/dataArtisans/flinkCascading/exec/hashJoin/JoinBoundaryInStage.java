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

package com.dataArtisans.flinkCascading.exec.hashJoin;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.ElementStage;
import cascading.flow.stream.element.InputSource;
import cascading.tuple.Tuple;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;

public class JoinBoundaryInStage extends ElementStage<Void, Tuple2<Tuple, Tuple[]>> implements InputSource {

	public JoinBoundaryInStage(FlowProcess flowProcess, FlowElement flowElement) {
		super(flowProcess, flowElement);
	}

	@Override
	public void receive(Duct previous, Void v) {
		throw new UnsupportedOperationException( "use run() instead" );
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run(Object input) throws Throwable {

		if(!(input instanceof Iterator)) {
			throw new InvalidArgumentException("FlinkMapInStage requires Iterator<Tuple>");
		}

		Iterator<Tuple2<Tuple, Tuple[]>> joinInputIterator = (Iterator<Tuple2<Tuple, Tuple[]>>)input;

		next.start(this);

		while (joinInputIterator.hasNext()) {

			Tuple2<Tuple, Tuple[]> joinListTuple;

			try
			{
				joinListTuple = joinInputIterator.next();
				flowProcess.increment( StepCounters.Tuples_Read, 1 );
				flowProcess.increment( SliceCounters.Tuples_Read, 1 );
			}
			catch( CascadingException exception ) {
				handleException( exception, null );
				continue;
			}
			catch( Throwable throwable ) {
				handleException( new DuctException( "internal error", throwable ), null );
				continue;
			}

			next.receive( this, joinListTuple );

		}

		next.complete(this);

	}
}
