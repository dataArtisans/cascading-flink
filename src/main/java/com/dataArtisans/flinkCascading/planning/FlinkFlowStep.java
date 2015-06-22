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

package com.dataArtisans.flinkCascading.planning;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.management.state.ClientState;
import cascading.pipe.Boundary;
import cascading.pipe.GroupBy;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.FileTapInputFormat;
import com.dataArtisans.flinkCascading.exec.operators.FileTapOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.Reducer;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import com.dataArtisans.flinkCascading.exec.operators.Mapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class FlinkFlowStep extends BaseFlowStep<Configuration> {

	private ExecutionEnvironment env;

	public FlinkFlowStep(ExecutionEnvironment env, ElementGraph elementGraph, FlowNodeGraph flowNodeGraph) {
		super( elementGraph, flowNodeGraph );
		this.env = env;
	}

	// Configures the MapReduce program for this step
	public Configuration createInitializedConfig( FlowProcess<Configuration> flowProcess, Configuration parentConfig ) {

		this.buildFlinkProgram();

		// TODO
		return null;
	}

	protected FlowStepJob<Configuration> createFlowStepJob( ClientState clientState, FlowProcess<Configuration> flowProcess, Configuration initializedStepConfig )
	{
		try {
			return new FlinkFlowStepJob(clientState, this, initializedStepConfig);
		}
		catch(NoClassDefFoundError error) {
			PlatformInfo platformInfo = HadoopUtil.getPlatformInfo();

//			String message = "unable to load platform specific class, please verify Hadoop cluster version: '%s', matches the Hadoop platform build dependency and associated FlowConnector, cascading-hadoop or cascading-hadoop2-mr1";
			String message = "Error"; // TODO

			logError( String.format( message, platformInfo.toString() ), error );

			throw error;
		}
	}

	/**
	 * Method clean removes any temporary files used by this FlowStep instance. It will log any IOExceptions thrown.
	 *
	 * @param config of type JobConf
	 */
	public void clean( Configuration config ) {

		// TODO: Do some clean-up. Check HadoopFlowStep for details.
	}

	public ExecutionEnvironment getExecutionEnvironment() {
		return this.env;
	}

	public JavaPlan getFlinkPlan() {
		return this.env.createProgramPlan();
	}

	private void printFlowStep() {
		Iterator<FlowNode> iterator = getFlowNodeGraph().getOrderedTopologicalIterator();

		System.out.println("Step Cnt: "+getFlowNodeGraph().vertexSet().size());
		System.out.println("Edge Cnt: "+getFlowNodeGraph().edgeSet().size());
		System.out.println("Src Set: "+getFlowNodeGraph().getSourceElements());
		System.out.println("Snk Set: "+getFlowNodeGraph().getSinkElements());
		System.out.println("##############");

		while(iterator.hasNext()) {

			FlowNode next = iterator.next();

			System.out.println("Node cnt: "+next.getElementGraph().vertexSet().size());
			System.out.println("Edge cnt: "+next.getElementGraph().edgeSet().size());

			System.out.println("Nodes: "+next.getElementGraph().vertexSet());

			System.out.println("-----------");
		}


	}

	public void buildFlinkProgram() {

		printFlowStep();

		FlowNodeGraph flowNodeGraph = getFlowNodeGraph();
		Iterator<FlowNode> iterator = flowNodeGraph.getOrderedTopologicalIterator();

		DataSet<Tuple> flinkPlan = null;

		while(iterator.hasNext()) {
			FlowNode node = iterator.next();

			Set<FlowElement> nodeSources = node.getSourceElements();
			if(nodeSources.size() != 1) {
				throw new RuntimeException("Only nodes with one input supported right now"); // TODO
			}
			Set<FlowElement> nodeSinks = node.getSinkElements();
			if(nodeSinks.size() != 1) {
				throw new RuntimeException("Only nodes with one output supported right now"); // TODO
			}

			FlowElement source = nodeSources.iterator().next();
			FlowElement sink = nodeSinks.iterator().next();

			// SOURCE
			if(source instanceof Tap && sink instanceof Boundary && ((Tap)source).isSource()) {

				// add data source to Flink program
				if(source instanceof FileTap) {

					FileTap tap = (FileTap)source;

					Properties conf = new Properties();
					tap.getScheme().sourceConfInit(null, tap, conf);

					flinkPlan = env
							.createInput(new FileTapInputFormat(tap, conf), new CascadingTupleTypeInfo(tap.getSourceFields()))
							.name(tap.getIdentifier())
							.setParallelism(1);
				}
				else {
					throw new RuntimeException("Only File taps supported right now");
				}
			}
			// SINK
			else if(source instanceof Boundary && sink instanceof Tap && ((Tap)sink).isSink()) {

				Tap tap = (Tap)sink;
				Fields tapFields = tap.getSinkFields();

				// check that no projection is necessary
				if(!tapFields.isAll()) {
					throw new RuntimeException("WOOPS!");
//						Scope scope = getIncomingScopeFrom(inputOps.get(0));
//						Fields tailFields = scope.getIncomingTapFields();
//
//						// check if we need to project
//						if(!tapFields.equalsFields(tailFields)) {
//							// add projection mapper
//							tail = tail
//									.map(new ProjectionMapper(tailFields, tapFields))
//									.returns(new CascadingTupleTypeInfo())
//									.name("Tap Projection Mapper");
//						}
				}

				if(tap instanceof FileTap) {

					FileTap fileTap = (FileTap) tap;
					Properties props = new Properties();

					flinkPlan
							.output(new FileTapOutputFormat(fileTap, tapFields, props))
							.setParallelism(1);

				}
				else {
					throw new RuntimeException("Only FileTaps supported right now."); // TODO
				}

			}
			// REDUCE
			else if(source instanceof GroupBy) {

				GroupBy groupBy = (GroupBy)source;

				if(groupBy.getKeySelectors().size() != 1) {
					throw new RuntimeException("Currently only groupby with single input supported");
				}
				Fields keyFields = groupBy.getKeySelectors().entrySet().iterator().next().getValue();
				int numKeys = keyFields.size();
				String[] keys = new String[numKeys];
				System.out.println("GroupBy keys");
				for(int i=0; i<numKeys; i++) {
					keys[i] = keyFields.get(i).toString();
					System.out.println(keys[i]);
				}

				flinkPlan = flinkPlan.groupBy(keys)
						.reduceGroup(new Reducer(node))
						.returns(new CascadingTupleTypeInfo(new Fields("token"))); // TODO

//				throw new RuntimeException("Reduce not yet supported");
			}
			// MAP
			else if(source instanceof Boundary) {

				Collection<Scope> inScopes = (Collection<Scope>) node.getPreviousScopes(sink);
				if(inScopes.size() != 1) {
					throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
				}
				Scope inScope = inScopes.iterator().next();

				System.out.println("Map output fields: "+inScope.getOutValuesFields());

				// if none of the above, its a Mapper
				flinkPlan = flinkPlan
						.mapPartition(new Mapper(node))
						.returns(new CascadingTupleTypeInfo(inScope.getOutValuesFields()));
			}

		}

	}

}
