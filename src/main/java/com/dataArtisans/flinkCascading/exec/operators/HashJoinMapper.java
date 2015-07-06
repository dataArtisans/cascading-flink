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

package com.dataArtisans.flinkCascading.exec.operators;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.hadoop.FlowMapper;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.pipe.Boundary;
import cascading.pipe.Pipe;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.exec.FlinkHashJoinStreamGraph;
import com.dataArtisans.flinkCascading.exec.ducts.BoundaryInStage;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HashJoinMapper extends RichMapPartitionFunction<Tuple, Tuple> {

	private static final Logger LOG = LoggerFactory.getLogger(FlowMapper.class);

	private FlowNode flowNode;
	private String[] inputIds;

	private FlinkHashJoinStreamGraph streamGraph;

	private BoundaryInStage sourceStage;

	private FlinkFlowProcess currentProcess;

	public HashJoinMapper() {}

	public HashJoinMapper(FlowNode flowNode, String[] inputIds) {

		this.flowNode = flowNode;
		this.inputIds = inputIds;
	}

	@Override
	public void open(Configuration config) {

		try {

			currentProcess = new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config));

			FlowElement sourceElement = null;

			for(FlowElement source : flowNode.getSourceElements()) {
				if(((Pipe)source).getName().equals(inputIds[0])) {
					sourceElement = source;
					break;
				}
			}

			if(sourceElement == null || !(sourceElement instanceof Boundary)) {
				throw new RuntimeException("Source of Mapper must be a Boundary");
			}
			Boundary source = (Boundary)sourceElement;

			streamGraph = new FlinkHashJoinStreamGraph( currentProcess, flowNode, source );

			sourceStage = this.streamGraph.getSourceStage();

			for( Duct head : streamGraph.getHeads() )
				LOG.info( "sourcing from: " + ( (ElementDuct) head ).getFlowElement() );

			for( Duct tail : streamGraph.getTails() )
				LOG.info( "sinking to: " + ( (ElementDuct) tail ).getFlowElement() );
		}
		catch( Throwable throwable ) {

			if( throwable instanceof CascadingException) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during mapper configuration", throwable );
		}

	}

	@Override
	public void mapPartition(Iterable<Tuple> input, Collector<Tuple> output) throws Exception {

//		currentProcess.setReporter( reporter );

		this.streamGraph.setTupleCollector(output);

		streamGraph.prepare();

		streamGraph.getHashJoinGate().buildHashtable(this.getRuntimeContext(), inputIds);

		long processBeginTime = System.currentTimeMillis();

//		currentProcess.increment( SliceCounters.Process_Begin_Time, processBeginTime );

		try {
			try {

				sourceStage.run( input.iterator() );

				sourceStage.complete(null);
			}
			catch( OutOfMemoryError error ) {
				throw error;
			}
			catch( IOException exception ) {
//				reportIfLocal( exception );
				throw exception;
			}
			catch( Throwable throwable ) {
//				reportIfLocal( throwable );

				if( throwable instanceof CascadingException ) {
					throw (CascadingException) throwable;
				}

				throw new FlowException( "internal error during mapper execution", throwable );
			}
		}
		finally
		{
			try
			{
				streamGraph.cleanup();
			}
			finally
			{
				long processEndTime = System.currentTimeMillis();

//				currentProcess.increment( SliceCounters.Process_End_Time, processEndTime );
//				currentProcess.increment( SliceCounters.Process_Duration, processEndTime - processBeginTime );
			}
		}

	}


	@Override
	public void close() {

	}

}
