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

package com.dataArtisans.flinkCascading.runtime.hashJoin;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.SliceCounters;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.pipe.Boundary;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.runtime.util.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
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

public class BinaryHashJoinJoiner extends RichFlatJoinFunction<Tuple, Tuple, Tuple> {

	private static final Logger LOG = LoggerFactory.getLogger(BinaryHashJoinJoiner.class);

	private FlowNode flowNode;
	private int[] joinKeyPos;

	private transient HashJoinStreamGraph streamGraph;
	private transient FlinkFlowProcess currentProcess;
	private JoinBoundaryInStage sourceStage;
	private transient Tuple[] joinedTuples;
	private transient Tuple2<Tuple, Tuple[]> joinInput;

	private transient long processBeginTime;
	private transient boolean prepareCalled;

	public BinaryHashJoinJoiner() {}

	public BinaryHashJoinJoiner(FlowNode flowNode, int[] joinKeys) {
		this.flowNode = flowNode;
		this.joinKeyPos = joinKeys;
	}

	@Override
	public void open(Configuration config) {

		try {

			this.joinedTuples = new Tuple[2];
			this.joinInput = new Tuple2<Tuple, Tuple[]>(new Tuple(), this.joinedTuples);

			currentProcess = new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config), getRuntimeContext(), flowNode.getID());

			Set<FlowElement> sources = flowNode.getSourceElements();
			// pick one (arbitrary) source
			FlowElement sourceElement = sources.iterator().next();
			if(!(sourceElement instanceof Boundary)) {
				throw new RuntimeException("Source of BinaryHashJoinJoiner must be a boundary");
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

			throw new FlowException( "internal error during BinaryHashJoinJoiner configuration", throwable );
		}

		this.prepareCalled = false;

	}

	@Override
	public void join(Tuple left, Tuple right, Collector<Tuple> output) throws Exception {

		if(!this.prepareCalled) {

			streamGraph.prepare();
			sourceStage.start(null);

			processBeginTime = System.currentTimeMillis();
			currentProcess.increment(SliceCounters.Process_Begin_Time, processBeginTime);
			prepareCalled = true;
		}

		this.streamGraph.setTupleCollector(output);

		joinInput.f0 = left.get(joinKeyPos);
		joinedTuples[0] = left;
		joinedTuples[1] = right;

		try {
			sourceStage.run(joinInput);
		}
		catch(IOException exception ) {
			throw exception;
		}
		catch( Throwable throwable ) {

			if( throwable instanceof CascadingException ) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during BinaryHashJoinJoiner execution", throwable );
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
