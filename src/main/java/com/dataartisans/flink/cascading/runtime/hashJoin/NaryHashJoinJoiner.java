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

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.SliceCounters;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.pipe.Boundary;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import com.dataartisans.flink.cascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static cascading.util.LogUtil.logCounters;
import static cascading.util.LogUtil.logMemory;

public class NaryHashJoinJoiner extends RichFlatJoinFunction<Tuple2<Tuple, Tuple[]>, Tuple, Tuple> {

	private static final Logger LOG = LoggerFactory.getLogger(NaryHashJoinJoiner.class);

	private FlowNode flowNode;
	private int numJoinInputs;

	private transient HashJoinStreamGraph streamGraph;
	private transient FlinkFlowProcess currentProcess;
	private JoinBoundaryInStage sourceStage;
	private transient Tuple[] joinedTuples;

	private transient long processBeginTime;
	private transient boolean prepareCalled;

	public NaryHashJoinJoiner() {}

	public NaryHashJoinJoiner(FlowNode flowNode, int numJoinInputs) {
		this.flowNode = flowNode;
		this.numJoinInputs = numJoinInputs;
	}

	@Override
	public void open(Configuration config) {

		try {

			joinedTuples = new Tuple[numJoinInputs];

			currentProcess = new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config), getRuntimeContext(), flowNode.getID());

			Set<FlowElement> sources = flowNode.getSourceElements();
			// pick one (arbitrary) source
			FlowElement sourceElement = sources.iterator().next();
			if(!(sourceElement instanceof Boundary)) {
				throw new RuntimeException("Source of NaryHashJoinJoiner must be a boundary");
			}

			Boundary source = (Boundary)sourceElement;

			streamGraph = new HashJoinStreamGraph( currentProcess, flowNode, source );
			sourceStage = this.streamGraph.getSourceStage();

			for( Duct head : streamGraph.getHeads() ) {
				LOG.info("sourcing from: " + ((ElementDuct) head).getFlowElement());
			}

			for( Duct tail : streamGraph.getTails() ) {
				LOG.info("sinking to: " + ((ElementDuct) tail).getFlowElement());
			}
		}
		catch( Throwable throwable ) {

			if( throwable instanceof CascadingException) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during NaryHashJoinJoiner configuration", throwable );
		}

		this.prepareCalled = false;

	}

	@Override
	public void join(Tuple2<Tuple, Tuple[]> left, Tuple right, Collector<Tuple> output) throws Exception {

		if(!this.prepareCalled) {

			streamGraph.prepare();
			sourceStage.start(null);

			processBeginTime = System.currentTimeMillis();
			currentProcess.increment(SliceCounters.Process_Begin_Time, processBeginTime);
			prepareCalled = true;
		}

		this.streamGraph.setTupleCollector(output);

		for(int i=0; i<numJoinInputs-1; i++) {
			joinedTuples[i] = left.f1[i];
		}
		joinedTuples[numJoinInputs-1] = right;
		left.f1 = joinedTuples;

		try {
			sourceStage.run(left);
		}
		catch(IOException exception ) {
			throw exception;
		}
		catch( Throwable throwable ) {

			if( throwable instanceof CascadingException ) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during NaryHashJoinJoiner execution", throwable );
		}
	}

	@Override
	public void close() {

		try {
			if( this.prepareCalled) {
				this.streamGraph.cleanup();
			}
		}
		finally {
			if( currentProcess != null ) {
				long processEndTime = System.currentTimeMillis();
				currentProcess.increment( SliceCounters.Process_End_Time, processEndTime );
				currentProcess.increment( SliceCounters.Process_Duration, processEndTime - processBeginTime );
			}

			String message = "flow node id: " + flowNode.getID();
			logMemory( LOG, message + ", mem on close" );
			logCounters( LOG, message + ", counter:", currentProcess );
		}
	}

}
