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

package com.dataArtisans.flinkCascading.exec.reducer;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.FlowMapper;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.pipe.GroupBy;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.util.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static cascading.util.LogUtil.logCounters;
import static cascading.util.LogUtil.logMemory;

/**
 *
 * Corresponds to FlowReducer
 *
 */
public class Reducer extends RichGroupReduceFunction<Tuple, Tuple> {

	private static final Logger LOG = LoggerFactory.getLogger(FlowMapper.class);

	private FlowNode flowNode;

	private FlinkReduceStreamGraph streamGraph;

	private GroupByInGate groupSource;

	private FlinkFlowProcess currentProcess;

	private boolean calledPrepare;

	public Reducer() {}

	public Reducer(FlowNode flowNode) {
		this.flowNode = flowNode;
	}

	@Override
	public void open(Configuration config) {

		this.calledPrepare = false;

		try {

			String taskId = "reduce-" + flowNode.getID();

			currentProcess = new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config), getRuntimeContext(), taskId);

			Set<FlowElement> sources = flowNode.getSourceElements();
			if(sources.size() != 1) {
				throw new RuntimeException("FlowNode for Mapper may only have a single source");
			}
			FlowElement sourceElement = sources.iterator().next();
			if(!(sourceElement instanceof GroupBy)) {
				throw new RuntimeException("Source of Reduce must be a GroupBy");
			}
			GroupBy source = (GroupBy)sourceElement;

			streamGraph = new FlinkReduceStreamGraph( currentProcess, flowNode, source );

			groupSource = this.streamGraph.getGroupSource();

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

			throw new FlowException( "internal error during mapper configuration", throwable );
		}

	}

	@Override
	public void reduce(Iterable<Tuple> input, Collector<Tuple> output) throws Exception {

//		currentProcess.setReporter( reporter );

		this.streamGraph.setTupleCollector(output);

		if(! this.calledPrepare) {
			this.streamGraph.prepare();
			this.calledPrepare = true;

			this.groupSource.start(this.groupSource);
		}

		long processBeginTime = System.currentTimeMillis();

//		currentProcess.increment( SliceCounters.Process_Begin_Time, processBeginTime );

		try
		{
			this.groupSource.run(input.iterator());
		}
		catch( OutOfMemoryError error )
		{
			throw error;
		}
		catch( Throwable throwable )
		{
//			reportIfLocal( throwable );

			if( throwable instanceof CascadingException ) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during reducer execution", throwable );
		}
	}


	@Override
	public void close() {

		try {
			if( this.calledPrepare)
			{
				this.groupSource.complete(this.groupSource);

				this.streamGraph.cleanup();
			}
		}
		finally {
			if( currentProcess != null )
			{
				long processEndTime = System.currentTimeMillis();
				currentProcess.increment( SliceCounters.Process_End_Time, processEndTime );
//				currentProcess.increment( SliceCounters.Process_Duration, processEndTime - processBeginTime );
			}

			String message = "flow node id: " + flowNode.getID();
			logMemory( LOG, message + ", mem on close" );
			logCounters( LOG, message + ", counter:", currentProcess );
		}
	}

}
