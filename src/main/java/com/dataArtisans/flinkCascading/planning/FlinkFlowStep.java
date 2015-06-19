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
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import com.dataArtisans.flinkCascading.exec.operators.Mapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.configuration.Configuration;

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

//		JobConf conf = parentConfig == null ? new JobConf() : HadoopUtil.copyJobConf(parentConfig); // TODO

		/*
		// disable warning
		conf.setBoolean( "mapred.used.genericoptionsparser", true );

		conf.setJobName( getStepDisplayName( conf.getInt( "cascading.display.id.truncate", Util.ID_LENGTH ) ) );

		conf.setOutputKeyClass( Tuple.class );
		conf.setOutputValueClass( Tuple.class );

		conf.setMapRunnerClass( FlowMapper.class );
		conf.setReducerClass( FlowReducer.class );

		// set for use by the shuffling phase
		TupleSerialization.setSerializations(conf);

		initFromSources( flowProcess, conf );

		initFromSink( flowProcess, conf );

		initFromTraps( flowProcess, conf );

		initFromProcessConfigDef( conf );

		int numSinkParts = getSink().getScheme().getNumSinkParts();

		if( numSinkParts != 0 )
		{
			// if no reducer, set num map tasks to control parts
			if( getGroup() != null )
				conf.setNumReduceTasks( numSinkParts );
			else
				conf.setNumMapTasks( numSinkParts );
		}
		else if( getGroup() != null )
		{
			int gatherPartitions = conf.getNumReduceTasks();

			if( gatherPartitions == 0 )
				gatherPartitions = conf.getInt( FlowRuntimeProps.GATHER_PARTITIONS, 0 );

			if( gatherPartitions == 0 )
				throw new FlowException( getName(), "a default number of gather partitions must be set, see FlowRuntimeProps" );

			conf.setNumReduceTasks( gatherPartitions );
		}

		conf.setOutputKeyComparatorClass( TupleComparator.class );

		if( getGroup() == null )
		{
			conf.setNumReduceTasks( 0 ); // disable reducers
		}
		else
		{
			// must set map output defaults when performing a reduce
			conf.setMapOutputKeyClass( Tuple.class );
			conf.setMapOutputValueClass( Tuple.class );
			conf.setPartitionerClass( GroupingPartitioner.class );

			// handles the case the groupby sort should be reversed
			if( getGroup().isSortReversed() )
				conf.setOutputKeyComparatorClass( ReverseTupleComparator.class );

			addComparators( conf, "cascading.group.comparator", getGroup().getKeySelectors(), this, getGroup() );

			if( getGroup().isGroupBy() )
				addComparators( conf, "cascading.sort.comparator", getGroup().getSortingSelectors(), this, getGroup() );

			if( !getGroup().isGroupBy() )
			{
				conf.setPartitionerClass( CoGroupingPartitioner.class );
				conf.setMapOutputKeyClass( IndexTuple.class ); // allows groups to be sorted by index
				conf.setMapOutputValueClass( IndexTuple.class );
				conf.setOutputKeyComparatorClass( IndexTupleCoGroupingComparator.class ); // sorts by group, then by index
				conf.setOutputValueGroupingComparator( CoGroupingComparator.class );
			}

			if( getGroup().isSorted() )
			{
				conf.setPartitionerClass( GroupingSortingPartitioner.class );
				conf.setMapOutputKeyClass( TuplePair.class );

				if( getGroup().isSortReversed() )
					conf.setOutputKeyComparatorClass( ReverseGroupingSortingComparator.class );
				else
					conf.setOutputKeyComparatorClass( GroupingSortingComparator.class );

				// no need to supply a reverse comparator, only equality is checked
				conf.setOutputValueGroupingComparator( GroupingComparator.class );
			}
		}

		// perform last so init above will pass to tasks
		String versionString = Version.getRelease();

		if( versionString != null )
			conf.set( "cascading.version", versionString );

		conf.set( CASCADING_FLOW_STEP_ID, getID() );
		conf.set( "cascading.flow.step.num", Integer.toString( getOrdinal() ) );

		HadoopUtil.setIsInflow( conf );

		Iterator<FlowNode> iterator = getFlowNodeGraph().getTopologicalIterator();

		String mapState = pack( iterator.next(), conf );
		String reduceState = pack( iterator.hasNext() ? iterator.next() : null, conf );

		// hadoop 20.2 doesn't like dist cache when using local mode
		int maxSize = Short.MAX_VALUE;

		int length = mapState.length() + reduceState.length();

		if( isHadoopLocalMode( conf ) || length < maxSize ) // seems safe
		{
			conf.set( "cascading.flow.step.node.map", mapState );

			if( !Util.isEmpty( reduceState ) )
				conf.set( "cascading.flow.step.node.reduce", reduceState );
		}
		else
		{
			conf.set( "cascading.flow.step.node.map.path", HadoopMRUtil.writeStateToDistCache(conf, getID(), "map", mapState) );

			if( !Util.isEmpty( reduceState ) )
				conf.set( "cascading.flow.step.node.reduce.path", HadoopMRUtil.writeStateToDistCache( conf, getID(), "reduce", reduceState ) );
		}

		return conf;

		*/

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
							.createInput(new FileTapInputFormat(tap, conf), new CascadingTupleTypeInfo())
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
					System.out.println("Compile to Reduce: "+node.getElementGraph().vertexSet());
				}
			// MAP
			else if(source instanceof Boundary) {
				// if none of the above, its a Mapper
				flinkPlan = flinkPlan
						.mapPartition(new Mapper(node))
						.returns(new CascadingTupleTypeInfo());
			}

		}

	}

}
