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
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.management.state.ClientState;
import cascading.pipe.Boundary;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.FileTapInputFormat;
import com.dataArtisans.flinkCascading.exec.operators.FileTapOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.Reducer;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import com.dataArtisans.flinkCascading.exec.operators.Mapper;
import com.dataArtisans.flinkCascading.exec.operators.ProjectionMapper;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
		Iterator<FlowNode> iterator = getFlowNodeGraph().getTopologicalIterator();

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
		Iterator<FlowNode> iterator = flowNodeGraph.getTopologicalIterator(); // TODO: topologicalIterator is non-deterministically broken!!!

		Map<FlowElement, DataSet<Tuple>> flinkFlows = new HashMap<FlowElement, DataSet<Tuple>>();

		while(iterator.hasNext()) {
			FlowNode node = iterator.next();

			Set<FlowElement> sources = getSources(node);

			if(sources.size() == 1) {

				// single input node: Map, Reduce, Source, Sink

				FlowElement source = getSource(node);
				Set<FlowElement> sinks = getSinks(node);

				// SOURCE
				if (source instanceof Tap
						&& ((Tap) source).isSource()) {

					DataSet<Tuple> sourceFlow = translateSource(node, env);
					for(FlowElement sink : sinks) {
						flinkFlows.put(sink, sourceFlow);
					}
				}
				// SINK
				else if (source instanceof Boundary
						&& sinks.size() == 1
						&& sinks.iterator().next() instanceof Tap) {

					DataSet<Tuple> input = flinkFlows.get(source);
					translateSink(input, node);
				}
				else if (source instanceof Boundary
						&& sinks.size() > 1
						// only sinks + source + head + tail
						&& node.getElementGraph().vertexSet().size() == sinks.size() + 1 + 2 ) {

					// just forward
					for(FlowElement sink : sinks) {
						flinkFlows.put(sink, flinkFlows.get(source));
					}

				}
				// REDUCE
				else if (source instanceof GroupBy) {

					DataSet<Tuple> input = flinkFlows.get(source);
					DataSet<Tuple> grouped = translateReduce(input, node);
					for(FlowElement sink : sinks) {
						flinkFlows.put(sink, grouped);
					}
				}
				// MAP
				else if (source instanceof Boundary) {

					DataSet<Tuple> input = flinkFlows.get(source);
					DataSet<Tuple> mapped = translateMap(input, node);
					for(FlowElement sink : sinks) {
						flinkFlows.put(sink, mapped);
					}
				}

			}
			else {

				// multi input node: Merge, (CoGroup, Join)

				boolean allSourcesBoundaries = true;
				for(FlowElement source : sources) {
					if(!(source instanceof Boundary)) {
						allSourcesBoundaries = false;
						break;
					}
				}

				Set<FlowElement> sinks = getSinks(node);
				// MERGE
				if(allSourcesBoundaries &&
						// only sources + sink + one more node (Merge) + head + tail
						node.getElementGraph().vertexSet().size() == sources.size() + 4) {

					DataSet<Tuple> unioned = translateMerge(flinkFlows, node);
					for(FlowElement sink : sinks) {
						flinkFlows.put(sink, unioned);
					}

				}
				else {
					throw new UnsupportedOperationException("No multi-input nodes other than Merge supported right now.");
				}


			}

		}

	}

	private DataSet<Tuple> translateSource(FlowNode node, ExecutionEnvironment env) {

		FlowElement source = getSource(node);

		// add data source to Flink program
		if(source instanceof FileTap) {
			return this.translateFileTapSource((FileTap)source, env);
		}
		else if(source instanceof MultiSourceTap) {
			return this.translateMultiSourceTap((MultiSourceTap)source, env);
		}
		else {
			throw new RuntimeException("Unsupported tap type encountered"); // TODO
		}

	}

	private DataSet translateFileTapSource(FileTap tap, ExecutionEnvironment env) {

		Properties conf = new Properties();
		tap.getScheme().sourceConfInit(null, tap, conf);

		DataSet<Tuple> src = env
				.createInput(new FileTapInputFormat(tap, conf), new CascadingTupleTypeInfo(tap.getSourceFields()))
				.name(tap.getIdentifier())
				.setParallelism(1);

		return src;
	}

	private DataSet translateMultiSourceTap(MultiSourceTap tap, ExecutionEnvironment env) {

		Iterator<Tap> childTaps = ((MultiSourceTap)tap).getChildTaps();

		DataSet cur = null;
		while(childTaps.hasNext()) {
			Tap childTap = childTaps.next();
			DataSet source;

			if(childTap instanceof FileTap) {
				source = translateFileTapSource((FileTap)childTap, env);
			}
			else {
				throw new RuntimeException("Tap type "+tap.getClass().getCanonicalName()+" not supported yet.");
			}

			if(cur == null) {
				cur = source;
			}
			else {
				cur = cur.union(source);
			}
		}

		return cur;
	}

	private void translateSink(DataSet<Tuple> input, FlowNode node) {

		FlowElement sink = getSink(node);

		if(!(sink instanceof Tap)) {
			throw new IllegalArgumentException("FlowNode is not a sink");
		}

		Tap sinkTap = (Tap)sink;

		Fields tapFields = sinkTap.getSinkFields();
		// check that no projection is necessary
		if(!tapFields.isAll()) {

			Scope inScope = getInScope(node);
			Fields tailFields = inScope.getIncomingTapFields();

			// check if we need to project
			if(!tapFields.equalsFields(tailFields)) {
				// add projection mapper
				input = input
						.map(new ProjectionMapper(tailFields, tapFields))
						.returns(new CascadingTupleTypeInfo(tapFields));
			}
		}

		if(sinkTap instanceof FileTap) {
			translateFileSinkTap(input, (FileTap) sinkTap);
		}
		else {
			throw new UnsupportedOperationException("Only file taps as sinks suppported right now.");
		}

	}

	private void translateFileSinkTap(DataSet<Tuple> input, FileTap fileSink) {

		Properties props = new Properties();
		input
				.output(new FileTapOutputFormat(fileSink, fileSink.getSinkFields(), props))
				.setParallelism(1);
	}

	private DataSet translateMap(DataSet<Tuple> input, FlowNode node) {

		Scope outScope = getFirstOutScope(node);

		return input
				.mapPartition(new Mapper(node))
				.withParameters(this.getConfig())
				.returns(new CascadingTupleTypeInfo(outScope.getOutValuesFields()));

	}

	private DataSet translateReduce(DataSet<Tuple> input, FlowNode node) {

		GroupBy groupBy = (GroupBy)getSource(node);

		Scope inScope = getInScope(node);
		Scope outScope = getOutScope(node);

		if(groupBy.getKeySelectors().size() != 1) {
			throw new RuntimeException("Currently only groupBy with single input supported");
		}

		// get grouping keys
		Fields keyFields = groupBy.getKeySelectors().entrySet().iterator().next().getValue();
		int numKeys = keyFields.size();
		String[] groupKeys = new String[numKeys];
		for(int i=0; i<numKeys; i++) {

			Comparable keyField = keyFields.get(i);
			if(keyField instanceof Integer) {
				keyField = inScope.getOutValuesFields().get((Integer) keyField);
			}
			groupKeys[i] = keyField.toString();
		}
		Comparator[] groupComps = keyFields.getComparators();

		// get group sorting keys
		Map<String, Fields> sortingSelectors = groupBy.getSortingSelectors();
		String[] sortKeys = null;
		Comparator[] sortComps = null;
		if(sortingSelectors.size() > 0) {
			Fields sortFields = groupBy.getSortingSelectors().entrySet().iterator().next().getValue();
			int numSortKeys = sortFields.size();
			sortKeys = new String[numSortKeys];
			for (int i = 0; i < numSortKeys; i++) {

				Comparable sortKeyField = sortFields.get(i);
				if (sortKeyField instanceof Integer) {
					sortKeyField = inScope.getOutValuesFields().get((Integer) sortKeyField);
				}
				sortKeys[i] = sortKeyField.toString();
			}
			sortComps = sortFields.getComparators();
		}

		// set custom comparators for grouping keys
		if(groupComps != null && groupComps.length > 0) {
			CascadingTupleTypeInfo tupleType = (CascadingTupleTypeInfo)input.getType();

			for(int i=0; i<groupKeys.length; i++) {
				if(groupComps[i] != null) {
					tupleType.setFieldComparator(groupKeys[i], groupComps[i]);
				}
			}
		}
		// set custom comparators for sort keys
		if(sortComps != null && sortComps.length > 0) {
			CascadingTupleTypeInfo tupleType = (CascadingTupleTypeInfo)input.getType();

			for(int i=0; i<sortKeys.length; i++) {
				if(sortComps[i] != null) {
					tupleType.setFieldComparator(sortKeys[i], groupComps[i]);
				}
			}
		}

		// get output fields for type info
		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		// Reduce without group sorting
		if(sortKeys == null) {

			return input
					.groupBy(groupKeys)
					.reduceGroup(new Reducer(node))
					.withParameters(this.getConfig())
					.returns(new CascadingTupleTypeInfo(outFields));
		}
		// Reduce with group sorting
		else {

			SortedGrouping<Tuple> grouping = input
					.groupBy(groupKeys)
					.sortGroup(sortKeys[0], Order.ASCENDING);

			for(int i=1; i<sortKeys.length; i++) {
				grouping = grouping.sortGroup(sortKeys[i], Order.ASCENDING);
			}

			return grouping
					.reduceGroup(new Reducer(node))
					.withParameters(this.getConfig())
					.returns(new CascadingTupleTypeInfo(outFields));
		}

	}

	private DataSet<Tuple> translateMerge(Map<FlowElement, DataSet<Tuple>> flinkFlows, FlowNode node) {

		// check if remaining node is a Merge
		Set<FlowElement> elements = new HashSet(node.getElementGraph().vertexSet());
		elements.removeAll(getSources(node));
		elements.remove(getSink(node));

		for(FlowElement v : elements) {
			if(!(v instanceof Merge || v instanceof Extent)) {
				throw new RuntimeException("Unexpected non-merge element found.");
			}
		}

		// this node is just a merge wrapped in boundaries.
		// translate it to a Flink union

		Set<FlowElement> sources = getSources(node);

		DataSet<Tuple> unioned = null;
		for(FlowElement source : sources) {
			if(unioned == null) {
				unioned = flinkFlows.get(source);
			}
			else {
				unioned = unioned.union(flinkFlows.get(source));
			}
		}
		return unioned;
	}

	private Set<FlowElement> getSources(FlowNode node) {
		return node.getSourceElements();
	}

	private Set<FlowElement> getSinks(FlowNode node) {
		return node.getSinkElements();
	}

	private FlowElement getSource(FlowNode node) {
		Set<FlowElement> nodeSources = node.getSourceElements();
		if(nodeSources.size() != 1) {
			throw new RuntimeException("Only nodes with one input supported right now");
		}
		return nodeSources.iterator().next();
	}

	private FlowElement getSink(FlowNode node) {
		Set<FlowElement> nodeSinks = node.getSinkElements();
		if(nodeSinks.size() != 1) {
			throw new RuntimeException("Only nodes with one output supported right now");
		}
		return nodeSinks.iterator().next();
	}

	private Scope getInScope(FlowNode node) {

		FlowElement source = getSource(node);

		Collection<Scope> inScopes = (Collection<Scope>) node.getPreviousScopes(source);
		if(inScopes.size() != 1) {
			throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
		}
		return inScopes.iterator().next();
	}

	private Scope getOutScope(FlowNode node) {

		FlowElement sink = getSink(node);

		Collection<Scope> outScopes = (Collection<Scope>) node.getPreviousScopes(sink);
		if(outScopes.size() != 1) {
			throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
		}
		return outScopes.iterator().next();
	}

	private Scope getFirstOutScope(FlowNode node) {

		FlowElement firstSink = getSinks(node).iterator().next();

		Collection<Scope> outScopes = (Collection<Scope>) node.getPreviousScopes(firstSink);
		if(outScopes.size() != 1) {
			throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
		}
		return outScopes.iterator().next();

	}

}
