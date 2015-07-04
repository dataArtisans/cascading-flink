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
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducer;
import com.dataArtisans.flinkCascading.exec.operators.FileTapInputFormat;
import com.dataArtisans.flinkCascading.exec.operators.FileTapOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.HashJoinMapper;
import com.dataArtisans.flinkCascading.exec.operators.IdMapper;
import com.dataArtisans.flinkCascading.exec.operators.ReducerJoinKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.InnerJoiner;
import com.dataArtisans.flinkCascading.exec.operators.Reducer;
import com.dataArtisans.flinkCascading.exec.operators.Mapper;
import com.dataArtisans.flinkCascading.exec.operators.ProjectionMapper;
import com.dataArtisans.flinkCascading.types.tuple.TupleTypeInfo;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

		env.setParallelism(1); // TODO: set for tests that count groups

		printFlowStep();

		FlowNodeGraph flowNodeGraph = getFlowNodeGraph();
		Iterator<FlowNode> iterator = flowNodeGraph.getTopologicalIterator(); // TODO: topologicalIterator is non-deterministically broken!!!

//		Map<FlowElement, DataSet<Tuple>> flinkFlows = new HashMap<FlowElement, DataSet<Tuple>>();
		Map<FlowElement, List<DataSet<Tuple>>> flinkMemo = new HashMap<FlowElement, List<DataSet<Tuple>>>();

		while(iterator.hasNext()) {
			FlowNode node = iterator.next();

			Set<FlowElement> all = node.getElementGraph().vertexSet();
			Set<FlowElement> sources = getSources(node);
			Set<FlowElement> sinks = getSinks(node);
			Set<FlowElement> inner = getInnerElements(node);

			// SOURCE
			if (sources.size() == 1 &&
					allOfType(sources, Tap.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, Boundary.class)) {

				DataSet<Tuple> sourceFlow = translateSource(node, env);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, Collections.singletonList(sourceFlow));
				}
			}
			// SINK
			else if (sources.size() == 1 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, Tap.class)) {

				DataSet<Tuple> input = flinkMemo.get(getSingle(sources)).get(0);
				translateSink(input, node);
			}
			// SPLIT or EMPTY NODE (Single boundary source, one or more boundary sinks & no intermediate nodes)
			else if (sources.size() == 1 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() >= 1 &&
					allOfType(sinks, Boundary.class) &&
					inner.size() == 0 ) {

				// just forward
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, flinkMemo.get(getSingle(sources)));
				}

			}
			// INPUT OF GROUPBY (one or more boundary sources, single groupBy sink, no inner)
			else if(sources.size() > 0 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, GroupBy.class) &&
					inner.size() == 0) {

				GroupBy groupBy = (GroupBy)sinks.iterator().next();

				// register input of groupBy
				List<DataSet<Tuple>> groupByInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : sources) {
					groupByInputs.add(flinkMemo.get(e).get(0));
				}

				flinkMemo.put(groupBy, groupByInputs);
			}
			// GROUPBY (Single groupBy source)
			else if (sources.size() == 1 &&
					allOfType(sources, GroupBy.class)) {

				List<DataSet<Tuple>> inputs = flinkMemo.get(getSingle(sources));
				DataSet<Tuple> grouped = translateReduce(inputs, node);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, Collections.singletonList(grouped));
				}
			}
			// INPUT OF COGROUP (one or more boundary sources, single coGroup sink, no inner)
			else if(sources.size() > 0 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, CoGroup.class) &&
					inner.size() == 0) {

				CoGroup coGroup = (CoGroup)sinks.iterator().next();

				// register input of CoGroup
				List<DataSet<Tuple>> coGroupInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : getNodeInputsInOrder(node, coGroup)) {
					coGroupInputs.add(flinkMemo.get(e).get(0));
				}

				flinkMemo.put(coGroup, coGroupInputs);
			}
			// COGROUP (Single CoGroup source)
			else if (sources.size() == 1 &&
					allOfType(sources, CoGroup.class)) {

				List<DataSet<Tuple>> inputs = flinkMemo.get(getSingle(sources));
				DataSet<Tuple> coGrouped = translateCoGroup(inputs, node);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, Collections.singletonList(coGrouped));
				}
			}
			// MERGE (multiple boundary sources, single boundary sink, single merge inner)
			else if (sources.size() > 1 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, Boundary.class) &&
					inner.size() == 1 &&
					allOfType(inner, Merge.class)) {

				List<DataSet<Tuple>> mergeInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : sources) {
					mergeInputs.add(flinkMemo.get(e).get(0));
				}

				DataSet<Tuple> unioned = translateMerge(mergeInputs, node);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, Collections.singletonList(unioned));
				}
			}
			// HASHJOIN (One or more boundary source, single boundary sink, single hashjoin inner)
			else if(sources.size() > 0 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					sinks.iterator().next() instanceof Boundary &&
					inner.size() == 1 &&
					inner.iterator().next() instanceof HashJoin
					) {

				HashJoin join = (HashJoin)inner.iterator().next();

				List<DataSet<Tuple>> joinInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : getNodeInputsInOrder(node, join)) {
					joinInputs.add(flinkMemo.get(e).get(0));
				}

				DataSet<Tuple> joined = translateHashJoin(joinInputs, node);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, Collections.singletonList(joined));
				}

			}
			// MAP (Single boundary source AND nothing else matches)
			else if (sources.size() == 1 &&
					allOfType(sources, Boundary.class)) {

				DataSet<Tuple> input = flinkMemo.get(getSingle(sources)).get(0);
				DataSet<Tuple> mapped = translateMap(input, node);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, Collections.singletonList(mapped));
				}
			}
			else {
				throw new RuntimeException("Could not translate this node: "+node.getElementGraph().vertexSet());
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

	private DataSet<Tuple> translateFileTapSource(FileTap tap, ExecutionEnvironment env) {

		Properties conf = new Properties();
		tap.getScheme().sourceConfInit(null, tap, conf);

		DataSet<Tuple> src = env
				.createInput(new FileTapInputFormat(tap, conf), new TupleTypeInfo(tap.getSourceFields()))
				.name(tap.getIdentifier())
				.setParallelism(1);

		return src;
	}

	private DataSet<Tuple> translateMultiSourceTap(MultiSourceTap tap, ExecutionEnvironment env) {

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
						.returns(new TupleTypeInfo(tapFields));
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

	private DataSet<Tuple> translateMap(DataSet<Tuple> input, FlowNode node) {

		Scope outScope = getFirstOutScope(node);

		return input
				.mapPartition(new Mapper(node))
				.withParameters(this.getConfig())
				.returns(new TupleTypeInfo(outScope.getOutValuesFields()));

	}

	private DataSet<Tuple> translateReduce(List<DataSet<Tuple>> inputs, FlowNode node) {

		GroupBy groupBy = (GroupBy) node.getSourceElements().iterator().next();

		Scope outScope = getOutScope(node);
		List<Scope> inScopes = getInputScopes(node, groupBy);

		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		Fields groupKeyFields = null;
		Fields sortKeyFields = null;

		DataSet<Tuple> merged = null;

		for(int i=0; i<inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);
			// get input scope
			Scope inScope = inScopes.get(i);

			// get grouping keys
			groupKeyFields = groupBy.getKeySelectors().get(inScope.getName());
			// get group sorting keys
			sortKeyFields = groupBy.getSortingSelectors().get(inScope.getName());

			if(merged == null) {
				merged = input;
			}
			else {
				merged = merged.union(input);
			}
		}

		String[] groupKeys = registerKeyFields(merged, groupKeyFields);
		String[] sortKeys = null;
		if (sortKeyFields != null) {
			sortKeys = registerKeyFields(merged, sortKeyFields);
		}

		Order sortOrder;
		if(groupBy.isSortReversed()) {
			sortOrder = Order.DESCENDING;

			// prepartition and sort input
			// required because Cascading allows to specifiy the order of grouping keys

			DataSet<Tuple> partitioned = merged
					.partitionByHash(groupKeys);

			DataSet<Tuple> sorted = partitioned;
			for(int i=0; i<groupKeys.length; i++) {
				sorted = sorted.sortPartition(groupKeys[i], sortOrder);
			}
			if(sortKeys != null) {
				for(int i=0; i<sortKeys.length; i++) {
					sorted = sorted.sortPartition(sortKeys[i], sortOrder);
				}
			}

			merged = sorted;
		}
		else {
			sortOrder = Order.ASCENDING;
		}

		// Reduce without group sorting
		if(sortKeys == null) {

			return merged
					.groupBy(groupKeys)
					.reduceGroup(new Reducer(node))
					.withParameters(this.getConfig())
					.returns(new TupleTypeInfo(outFields));
		}
		// Reduce with group sorting
		else {

			SortedGrouping<Tuple> grouping = merged
					.groupBy(groupKeys)
					.sortGroup(sortKeys[0], sortOrder);

			for(int i=1; i<sortKeys.length; i++) {
				grouping = grouping.sortGroup(sortKeys[i], sortOrder);
			}

			return grouping
					.reduceGroup(new Reducer(node))
					.withParameters(this.getConfig())
					.returns(new TupleTypeInfo(outFields));
		}

	}

	private DataSet<Tuple> translateMerge(List<DataSet<Tuple>> inputs, FlowNode node) {

		DataSet<Tuple> unioned = null;
		TypeInformation<Tuple> type = null;

		for(DataSet<Tuple> input : inputs) {
			if(unioned == null) {
				unioned = input;
				type = input.getType();
			}
			else {
				unioned = unioned.union(input);
			}
		}
		return unioned.mapPartition(new IdMapper())
				.returns(type);

	}

	private DataSet<Tuple> translateCoGroup(List<DataSet<Tuple>> inputs, FlowNode node) {

		CoGroup coGroup = (CoGroup)node.getSourceElements().iterator().next();

		return translateCoGroupAsReduce(inputs, node);
		/*

		Joiner joiner = coGroup.getJoiner();
		if(joiner instanceof InnerJoin) {
			// handle inner join
			return this.translateInnerCoGroup(inputs, node);
		}
		else if(joiner instanceof LeftJoin) {
			// TODO handle left outer join
			throw new UnsupportedOperationException("Left outer join not supported yet");
		}
		else if(joiner instanceof RightJoin) {
			// TODO handle right outer join
			throw new UnsupportedOperationException("Right outer join not supported yet");
		}
		else if(joiner instanceof OuterJoin) {
			// TODO handle full outer join
			throw new UnsupportedOperationException("Full outer join not supported yet");
		}
		else if(joiner instanceof MixedJoin) {
			// TODO handle mixed join
			throw new UnsupportedOperationException("Mixed join not supported yet");
		}
		else if(joiner instanceof BufferJoin) {
			// TODO hanlde buffer join
			// translate to GroupBy
			throw new UnsupportedOperationException("Buffer join not supported yet");
		}
		else {
			// TODO handle user-defined join
			throw new UnsupportedOperationException("User-defined join not supported yet");
		}
		*/
	}

	private DataSet<Tuple> translateCoGroupAsReduce(List<DataSet<Tuple>> inputs, FlowNode node) {

		CoGroup coGroup = (CoGroup) node.getSourceElements().iterator().next();

		// prepare inputs: (extract keys and assign input id)
		DataSet<Tuple3<Tuple, Integer, Tuple>> groupByInput = null;

		Scope outScope = getOutScope(node);
		List<Scope> inScopes = getInputScopes(node, coGroup);
		TypeInformation<Tuple3<Tuple, Integer, Tuple>> keyedType = null;

		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		for(int i=0; i<inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);
			// get input scope
			Scope inputScope = inScopes.get(i);

			// get keys
			Fields inputFields = ((TupleTypeInfo)input.getType()).getFields();
			Fields joinKeyFields = coGroup.getKeySelectors().get(inputScope.getName());
			int[] keyPos = inputFields.getPos(joinKeyFields);

			if(joinKeyFields.isNone()) {
				// set default key
				joinKeyFields = new Fields("defaultKey");
			}

			if(keyedType == null) {
				keyedType = new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple3<Tuple, Integer, Tuple>>(
						new TupleTypeInfo(joinKeyFields),
						BasicTypeInfo.INT_TYPE_INFO,
						new TupleTypeInfo(inputFields)
				);
			}

			// add mapper
			DataSet<Tuple3<Tuple, Integer, Tuple>> keyedInput = input
					.map(new ReducerJoinKeyExtractor(i, keyPos))
					.returns(keyedType);

			// add to groupByInput
			if(groupByInput == null) {
				groupByInput = keyedInput;
			}
			else {
				groupByInput = groupByInput
						.union(keyedInput);
			}
		}

		return groupByInput
				.groupBy("f0.*")
				.sortGroup(1, Order.DESCENDING)
				.reduceGroup(new CoGroupReducer(node))
				.withParameters(getConfig())
				.returns(new TupleTypeInfo(outFields));

	}

	private DataSet<Tuple> translateInnerCoGroup(List<DataSet<Tuple>> inputs, FlowNode node) {

		CoGroup coGroup = (CoGroup) node.getSourceElements().iterator().next();
		Joiner joiner = coGroup.getJoiner();
		if (!(joiner instanceof InnerJoin)) {
			throw new IllegalArgumentException("CoGroup must have InnerJoiner");
		}
		if (coGroup.isSelfJoin()) {
			throw new UnsupportedOperationException("Self-join not supported yet");
		}
		if (inputs.size() > 2) {
			throw new UnsupportedOperationException("Only binary CoGroups supported yet");
		}

		DataSet<Tuple> joined = null;
		Fields resultFields = new Fields();
		String[] firstInputJoinKeys = null;

		// get result fields for each input
		List<Scope> inScopes = getInputScopes(node, coGroup);
		List<Fields> resultFieldsByInput = getResultFieldsByInput(coGroup, inScopes);

		// for each input
		for (int i = 0; i < inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);
			// get input scope
			Scope inputScope = inScopes.get(i);

			// get join keys
			Fields joinKeyFields = coGroup.getKeySelectors().get(inputScope.getName());
			String[] joinKeys = registerKeyFields(input, joinKeyFields);

			resultFields = resultFields.append(resultFieldsByInput.get(i));

			// first input
			if (joined == null) {

				joined = input;
				firstInputJoinKeys = joinKeys;

			// other inputs
			} else {

				joined = joined.join(input, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
						.where(firstInputJoinKeys).equalTo(joinKeys)
						.with(new InnerJoiner())
						.returns(new TupleTypeInfo(resultFields))
//						.withForwardedFieldsFirst(leftJoinKeys) // TODO
//						.withForwardedFieldsSecond(joinKeys) // TODO
						.withParameters(this.getConfig());

				// TODO: update firstInputJoinKeys, update leftJoinKeys

			}
		}
		return joined;

	}

	private DataSet<Tuple> translateHashJoin(List<DataSet<Tuple>> inputs, FlowNode node) {

		// TODO: add proper HashJoin implementation!
		return translatHashJoinAsMap(inputs, node);

	}

	private DataSet<Tuple> translatHashJoinAsMap(List<DataSet<Tuple>> inputs, FlowNode node) {

		Set<FlowElement> innerElements = getInnerElements(node);
		if(innerElements.size() != 1 && !(innerElements.iterator().next() instanceof HashJoin)) {
			throw new RuntimeException("Only one inner element allowed which must be a HashJoin.");
		}

		HashJoin hashJoin = (HashJoin)innerElements.iterator().next();
		FlowElement[] nodeInputs = getNodeInputsInOrder(node, hashJoin);

		String[] inputIds = new String[nodeInputs.length];
		for(int i=0; i<nodeInputs.length; i++) {
			inputIds[i] = ((Pipe)nodeInputs[i]).getName();
		}

		Scope outScope = getOutScope(node);

		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		MapPartitionOperator<Tuple, Tuple> joined = inputs.get(0)
				.mapPartition(new HashJoinMapper(node, inputIds))
				.withParameters(this.getConfig());
		for(int i=1; i<inputs.size(); i++) {
			joined.withBroadcastSet(inputs.get(i), inputIds[i]);
		}
		joined.returns(new TupleTypeInfo(outFields));

		return joined;

	}

	private List<Scope> getInputScopes(FlowNode node, Splice splice) {

		Pipe[] inputs = splice.getPrevious();
		List<Scope> inScopes = new ArrayList<Scope>(inputs.length);
		for(Pipe input : inputs) {
			boolean found = false;
			for (Scope inScope : node.getPreviousScopes(splice)) {
				if(inScope.getName().equals(input.getName())) {
					inScopes.add(inScope);
					found = true;
					break;
				}
			}
			if(!found) {
				throw new RuntimeException("Input scope was not found");
			}
		}

		return inScopes;
	}

	private List<Fields> getResultFieldsByInput(Splice splice, List<Scope> inScopes) {

		List<Fields> resultFieldsByInput = new ArrayList<Fields>();

		if(splice.getJoinDeclaredFields() == null) {
			for(int i=0; i<inScopes.size(); i++) {
				Fields resultFields = inScopes.get(i).getOutValuesFields();

				resultFieldsByInput.add(resultFields);
			}
		}
		else {
			int cnt = 0;
			Fields declaredFields = splice.getJoinDeclaredFields();
			for(int i=0; i<inScopes.size(); i++) {
				Fields inputFields = inScopes.get(i).getOutValuesFields();

				Fields resultFields = new Fields();

				for(int j=0; j<inputFields.size(); j++) {
					Comparable name = declaredFields.get(cnt++);
					Type type = inputFields.getType(j);

					if(type != null) {
						resultFields = resultFields.append(new Fields(name, type));
					}
					else {
						resultFields = resultFields.append(new Fields(name));
					}
				}

				resultFieldsByInput.add(resultFields);
			}
		}

		return resultFieldsByInput;
	}

	private FlowElement[] getNodeInputsInOrder(FlowNode node, Splice splice) {

		Map<String, Integer> posMap = splice.getPipePos();
		FlowElement[] spliceInputs = new FlowElement[posMap.size()];
		ElementGraph eg = node.getElementGraph();

		for(FlowElement nodeSource : getSources(node)) {
			int idx = posMap.get(eg.getEdge(nodeSource, splice).getName());
			spliceInputs[idx] = nodeSource;
		}

		return spliceInputs;
	}


	private Set<FlowElement> getSources(FlowNode node) {
		return node.getSourceElements();
	}

	private Set<FlowElement> getSinks(FlowNode node) {
		return node.getSinkElements();
	}

	private Set<FlowElement> getInnerElements(FlowNode node) {
		Set<FlowElement> inner = new HashSet(node.getElementGraph().vertexSet());
		inner.removeAll(getSources(node));
		inner.removeAll(getSinks(node));
		Set<FlowElement> toRemove = new HashSet<FlowElement>();
		for(FlowElement e : inner) {
			if(e instanceof Extent) {
				toRemove.add(e);
			}
		}
		inner.removeAll(toRemove);
		return inner;
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

	private boolean allOfType(Set<FlowElement> set, Class<? extends FlowElement> type) {

		for(FlowElement e : set) {
			if(!(type.isInstance(e))) {
				return false;
			}
		}
		return true;
	}

	private <X> X getSingle(Set<X> set) {
		if(set.size() != 1) {
			throw new RuntimeException("Set size > 1");
		}
		return set.iterator().next();
	}

	private String[] registerKeyFields(DataSet<Tuple> input, Fields keyFields) {
		return ((TupleTypeInfo)input.getType()).registerKeyFields(keyFields);
	}

}
