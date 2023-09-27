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

package com.dataartisans.flink.cascading.runtime.sink;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.pipe.Boundary;
import cascading.tap.Tap;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import com.dataartisans.flink.cascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;

public class TapOutputFormat extends RichOutputFormat<Tuple> implements FinalizeOnMaster {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(TapOutputFormat.class);

	private final FlowNode flowNode;

	private transient org.apache.hadoop.conf.Configuration config;
	private transient FlinkFlowProcess flowProcess;
	private transient SinkStreamGraph streamGraph;
	private transient SinkBoundaryInStage sourceStage;

	private transient long processBeginTime;

	public TapOutputFormat(FlowNode node) {
		super();
		this.flowNode = node;
	}

	@Override
	public void configure(Configuration config) {

		this.config = FlinkConfigConverter.toHadoopConfig(config);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		this.processBeginTime = System.currentTimeMillis();

		BigInteger numId = new BigInteger(flowNode.getID(), 16);
		String hadoopTaskId = String.format( "attempt_%012d_0000_%s_%06d_0", numId.longValue(), "m", taskNumber );

		this.config.setInt("mapred.task.partition", taskNumber);
		this.config.set("mapred.task.id", hadoopTaskId);

		try {

			flowProcess = new FlinkFlowProcess(this.config, this.getRuntimeContext(), flowNode.getID());

			Set<FlowElement> sources = flowNode.getSourceElements();
			if(sources.size() != 1) {
				throw new RuntimeException("FlowNode for TapOutputFormat may only have a single source");
			}
			FlowElement sourceElement = sources.iterator().next();
			if(!(sourceElement instanceof Boundary)) {
				throw new RuntimeException("Source of TapOutputFormat must be a Boundary");
			}
			Boundary source = (Boundary)sourceElement;

			streamGraph = new SinkStreamGraph( flowProcess, flowNode, source );
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

			throw new FlowException( "internal error during TapOutputFormat configuration", throwable );
		}

		streamGraph.prepare();

	}

	@Override
	public void writeRecord(Tuple t) throws IOException {

		try {
			sourceStage.run( t );
		}
		catch( OutOfMemoryError error ) {
			throw error;
		}
		catch( IOException exception ) {
			throw exception;
		}
		catch( Throwable throwable ) {

			if( throwable instanceof CascadingException ) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during TapOutputFormat execution", throwable );
		}
	}

	@Override
	public void close() throws IOException {
		try {
			streamGraph.cleanup();
		}
		finally {
			long processEndTime = System.currentTimeMillis();

			flowProcess.increment( SliceCounters.Process_End_Time, processEndTime );
			flowProcess.increment( SliceCounters.Process_Duration, processEndTime - this.processBeginTime );
		}
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {

		org.apache.hadoop.conf.Configuration config = HadoopUtil.copyConfiguration(this.config);
		Tap tap = this.flowNode.getSinkTaps().iterator().next();

		config.setBoolean(HadoopUtil.CASCADING_FLOW_EXECUTING, false);
		HadoopUtil.setOutputPath(config, new Path(tap.getIdentifier()));

		Hadoop18TapUtil.cleanupJob( config );
	}
}
