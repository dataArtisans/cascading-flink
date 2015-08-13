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

package com.dataArtisans.flinkCascading.planner;

import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
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
import cascading.pipe.joiner.BufferJoin;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.property.ConfigDef;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.MultiInputFormat;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.runtime.coGroup.bufferJoin.CoGroupBufferReducer;
import com.dataArtisans.flinkCascading.runtime.coGroup.bufferJoin.BufferJoinKeyExtractor;
import com.dataArtisans.flinkCascading.runtime.coGroup.regularJoin.CoGroupReducer;
import com.dataArtisans.flinkCascading.runtime.coGroup.regularJoin.TupleAppendCoGrouper;
import com.dataArtisans.flinkCascading.runtime.hashJoin.JoinPrepareMapper;
import com.dataArtisans.flinkCascading.runtime.hashJoin.TupleAppendCrosser;
import com.dataArtisans.flinkCascading.runtime.hashJoin.TupleAppendJoiner;
import com.dataArtisans.flinkCascading.runtime.hashJoin.HashJoinMapper;
import com.dataArtisans.flinkCascading.runtime.each.EachMapper;
import com.dataArtisans.flinkCascading.runtime.sink.TapOutputFormat;
import com.dataArtisans.flinkCascading.runtime.source.TapInputFormat;
import com.dataArtisans.flinkCascading.runtime.util.IdMapper;
import com.dataArtisans.flinkCascading.runtime.groupBy.GroupByReducer;
import com.dataArtisans.flinkCascading.runtime.util.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.types.tuple.TupleTypeInfo;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FlinkFlowStep extends BaseFlowStep<Configuration> {

	private ExecutionEnvironment env;
	private List<String> classPath;

	public FlinkFlowStep(ExecutionEnvironment env, ElementGraph elementGraph, FlowNodeGraph flowNodeGraph, List<String> classPath) {
		super(elementGraph, flowNodeGraph);
		this.env = env;
		this.classPath = classPath;
	}

	// Configures the MapReduce program for this step
	public Configuration createInitializedConfig( FlowProcess<Configuration> flowProcess, Configuration parentConfig ) {

		Configuration config = parentConfig == null ? new JobConf() : HadoopUtil.copyJobConf( parentConfig );
		HadoopUtil.setIsInflow(config);

		this.setConfig(config);

		return config;
	}

	protected FlowStepJob<Configuration> createFlowStepJob( ClientState clientState, FlowProcess<Configuration> flowProcess, Configuration initializedStepConfig )
	{
		try {

			this.buildFlinkProgram(flowProcess);

			return new FlinkFlowStepJob(clientState, this, initializedStepConfig, classPath);
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

		System.out.println("Step Cnt: " + getFlowNodeGraph().vertexSet().size());
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

	public void buildFlinkProgram(FlowProcess flowProcess) {

		printFlowStep();

		int numMappers;
		try {
			numMappers = Integer.parseInt(((FlinkFlowProcess) flowProcess).getConfig().get("flink.num.mappers"));
		} catch (NumberFormatException e) {
			numMappers = -1;
		}

		int numReducers;
		try {
			numReducers = Integer.parseInt(((FlinkFlowProcess) flowProcess).getConfig().get("flink.num.reducers"));
		} catch (NumberFormatException e) {
			numReducers = -1;
		}

		numMappers = (numMappers > 0) ? numMappers : env.getParallelism();
		numReducers = (numReducers > 0) ? numReducers : env.getParallelism();

		FlowNodeGraph flowNodeGraph = getFlowNodeGraph();
		Iterator<FlowNode> iterator = flowNodeGraph.getTopologicalIterator(); // TODO: topologicalIterator is non-deterministically broken!!!

		Map<FlowElement, DataSet<?>> flinkMemo = new HashMap<FlowElement, DataSet<?>>();

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

				DataSet<Tuple> sourceFlow = translateSource(flowProcess, env, node, numMappers);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, sourceFlow);
				}
			}
			// SINK
			else if (sources.size() == 1 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, Tap.class)) {

				DataSet<Tuple> input = (DataSet<Tuple>) flinkMemo.get(getSingle(sources));
				translateSink(flowProcess, input, node);
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

				GroupBy groupBy = (GroupBy)getSingle(sinks);

				List<DataSet<Tuple>> groupByInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : sources) {
					groupByInputs.add((DataSet<Tuple>)flinkMemo.get(e));
				}

				// prepare groupBy input
				DataSet<Tuple> groupByInput = prepareGroupByInput(groupByInputs, node, numReducers);

				flinkMemo.put(groupBy, groupByInput);
			}
			// GROUPBY (Single groupBy source)
			else if (sources.size() == 1 &&
					allOfType(sources, GroupBy.class)) {

				DataSet<Tuple> input = (DataSet<Tuple>)flinkMemo.get(getSingle(sources));
				DataSet<Tuple> grouped = translateGroupBy(input, node, numReducers);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, grouped);
				}
			}
			// INPUT OF COGROUP (one or more boundary sources, single coGroup sink, no inner)
			else if(sources.size() > 0 &&
					allOfType(sources, Boundary.class) &&
					sinks.size() == 1 &&
					allOfType(sinks, CoGroup.class) &&
					inner.size() == 0) {

				CoGroup coGroup = (CoGroup)getSingle(sinks);

				List<DataSet<Tuple>> coGroupInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : getNodeInputsInOrder(node, coGroup)) {
					coGroupInputs.add((DataSet<Tuple>)flinkMemo.get(e));
				}

				// prepare coGroup input
				DataSet<?> input = prepareCoGroupInput(coGroupInputs, node, numReducers);
				flinkMemo.put(coGroup, input);
			}
			// COGROUP (Single CoGroup source)
			else if (sources.size() == 1 &&
					allOfType(sources, CoGroup.class)) {

				CoGroup coGroup = (CoGroup)getSingle(sources);

				DataSet<?> input = flinkMemo.get(coGroup);
				DataSet<Tuple> coGrouped = translateCoGroup(input, node, numReducers);

				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, coGrouped);
				}
			}
			// HASHJOIN (one or more boundary source, followed by a single HashJoin)
			else if(sources.size() > 0 &&
					allOfType(sources, Boundary.class) &&
					getCommonSuccessor(sources, node) instanceof HashJoin) {

				HashJoin hashJoin = (HashJoin)getCommonSuccessor(sources, node);
				List<DataSet<Tuple>> hashJoinInputs = new ArrayList<DataSet<Tuple>>(sources.size());
				for(FlowElement e : getNodeInputsInOrder(node, hashJoin)) {
					hashJoinInputs.add((DataSet<Tuple>)flinkMemo.get(e));
				}

				DataSet<Tuple> joined = translateHashJoin(hashJoinInputs, node, numMappers);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, joined);
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
					mergeInputs.add((DataSet<Tuple>)flinkMemo.get(e));
				}

				DataSet<Tuple> unioned = translateMerge(mergeInputs, node);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, unioned);
				}
			}
			// MAP (Single boundary source AND nothing else matches)
			else if (sources.size() == 1 &&
					allOfType(sources, Boundary.class)) {

				DataSet<Tuple> input = (DataSet<Tuple>)flinkMemo.get(getSingle(sources));
				DataSet<Tuple> mapped = translateMap(input, node, numMappers);
				for(FlowElement sink : sinks) {
					flinkMemo.put(sink, mapped);
				}
			}
			else {
				throw new RuntimeException("Could not translate this node: "+node.getElementGraph().vertexSet());
			}
		}

	}

	private DataSet<Tuple> translateSource(FlowProcess flowProcess, ExecutionEnvironment env, FlowNode node, int dop) {

		Tap tap = this.getSingle(node.getSourceTaps());
		JobConf tapConfig = new JobConf(this.getNodeConfig(node));
		tap.sourceConfInit(flowProcess, tapConfig);
		tapConfig.set( "cascading.step.source", Tap.id( tap ) );

		JobConf sourceConfig = new JobConf(this.getNodeConfig(node));
		MultiInputFormat.addInputFormat(sourceConfig, tapConfig);

		DataSet<Tuple> src = env
				.createInput(new TapInputFormat(node), new TupleTypeInfo(tap.getSourceFields()))
				.name(tap.getIdentifier())
				.setParallelism(dop)
				.withParameters(FlinkConfigConverter.toFlinkConfig(new Configuration(sourceConfig)));

		return src;

	}

	private void translateSink(FlowProcess flowProcess, DataSet<Tuple> input, FlowNode node) {

		Tap tap = this.getSingle(node.getSinkTaps());
		Configuration sinkConfig = this.getNodeConfig(node);
		tap.sinkConfInit(flowProcess, sinkConfig);

		int parallelism = ((Operator)input).getParallelism();

		input
				.output(new TapOutputFormat(node))
				.name(tap.getIdentifier())
				.setParallelism(parallelism)
				.withParameters(FlinkConfigConverter.toFlinkConfig(sinkConfig));

	}


	private DataSet<Tuple> translateMap(DataSet<Tuple> input, FlowNode node, int dop) {

		Scope outScope = getOutScope(node);

		return input
				.mapPartition(new EachMapper(node))
				.returns(new TupleTypeInfo(outScope.getOutValuesFields()))
				.withParameters(this.getFlinkNodeConfig(node))
				.setParallelism(dop)
				.name("map-" + node.getID());

	}

	private DataSet<Tuple> prepareGroupByInput(List<DataSet<Tuple>> inputs, FlowNode node, int dop) {

		DataSet<Tuple> merged = null;

		for(int i=0; i<inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);

			if(merged == null) {
				merged = input;
			}
			else {
				merged = merged
						.union(input)
						.setParallelism(dop);
			}
		}

		return merged;
	}

	private DataSet<Tuple> translateGroupBy(DataSet<Tuple> input, FlowNode node, int dop) {

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

		// get input scope
		Scope inScope = inScopes.get(0);

		// get grouping keys
		Fields groupKeyFields = groupBy.getKeySelectors().get(inScope.getName());
		// get group sorting keys
		Fields sortKeyFields = groupBy.getSortingSelectors().get(inScope.getName());

		String[] groupKeys = registerKeyFields(input, groupKeyFields);
		String[] sortKeys = null;
		if (sortKeyFields != null) {
			sortKeys = registerKeyFields(input, sortKeyFields);
		}
		Order sortOrder = groupBy.isSortReversed() ? Order.DESCENDING : Order.ASCENDING;

		DataSet<Tuple> result = input;

		// hash partition and sort on grouping keys if necessary
		if(groupKeys != null && groupKeys.length > 0) {
			// hash partition
			result = result
					.partitionByHash(groupKeys)
					.setParallelism(dop);

			// sort on grouping keys
			result = result
					.sortPartition(groupKeys[0], sortOrder)
					.setParallelism(dop);
			for(int i=1; i<groupKeys.length; i++) {
				result = result
						.sortPartition(groupKeys[i], sortOrder)
						.setParallelism(dop);
			}
		}

		// sort on sorting keys if necessary
		if(sortKeys != null && sortKeys.length > 0) {

			result = result
					.sortPartition(sortKeys[0], sortOrder)
					.setParallelism(dop);
			for(int i=1; i<sortKeys.length; i++) {
				result = result
						.sortPartition(sortKeys[i], sortOrder)
						.setParallelism(dop);
			}
		}

		// group by reduce
		if(groupKeys != null && groupKeys.length > 0) {

			return result
					.groupBy(groupKeys)
					.reduceGroup(new GroupByReducer(node))
					.returns(new TupleTypeInfo(outFields))
					.withParameters(this.getFlinkNodeConfig(node))
					.setParallelism(dop)
					.name("reduce-" + node.getID());

		}
		// all reduce (no group keys)
		else {
			// move all data to one partition before sorting
			if(sortKeys != null && sortKeys.length > 0) {
				((SortPartitionOperator)result).setParallelism(1);
			}

			// group all data
			return result
					.reduceGroup(new GroupByReducer(node))
					.returns(new TupleTypeInfo(outFields))
					.withParameters(this.getFlinkNodeConfig(node))
					.setParallelism(dop)
					.name("reduce-"+ node.getID());
		}

	}

	private DataSet<Tuple> translateMerge(List<DataSet<Tuple>> inputs, FlowNode node) {

		DataSet<Tuple> unioned = null;
		TypeInformation<Tuple> type = null;

		int maxDop = -1;

		for(DataSet<Tuple> input : inputs) {
			maxDop = Math.max(maxDop, ((Operator)input).getParallelism());
			if(unioned == null) {
				unioned = input;
				type = input.getType();
			}
			else {
				unioned = unioned.union(input);
			}
		}
		return unioned.mapPartition(new IdMapper())
				.returns(type)
				.setParallelism(maxDop);

	}

	private DataSet<?> prepareCoGroupInput(List<DataSet<Tuple>> inputs, FlowNode node, int dop) {

		CoGroup coGroup = (CoGroup)getSingle(node.getSinkElements());
		List<Scope> inScopes = getInputScopes(node, coGroup);

		Joiner joiner = coGroup.getJoiner();

		int numJoinInputs = coGroup.isSelfJoin() ? coGroup.getNumSelfJoins() + 1 : inputs.size();

		Fields[] inputFields = new Fields[numJoinInputs];
		Fields[] keyFields = new Fields[numJoinInputs];
		String[][] flinkKeys = new String[numJoinInputs][];
		List<DataSet<Tuple>> joinInputs;

		// collect key and value fields of inputs
		if(!coGroup.isSelfJoin()) {
			// regular join with different inputs

			for (int i = 0; i < numJoinInputs; i++) {
				// get input scope
				Scope inScope = inScopes.get(i);

				inputFields[i] = ((TupleTypeInfo)inputs.get(i).getType()).getFields();
				// get join key fields
				keyFields[i] = coGroup.getKeySelectors().get(inScope.getName());
				flinkKeys[i] = registerKeyFields(inputs.get(i), keyFields[i]);
			}

			joinInputs = inputs;
		}
		else {
			// self join

			Scope inScope = inScopes.get(0);

			inputFields[0] = ((TupleTypeInfo)inputs.get(0).getType()).getFields();
			// get join key fields
			keyFields[0] = coGroup.getKeySelectors().get(inScope.getName());
			flinkKeys[0] = registerKeyFields(inputs.get(0), keyFields[0]);

			for (int i = 1; i < numJoinInputs; i++) {
				inputFields[i] = inputFields[0];
				keyFields[i] = keyFields[0];
				flinkKeys[i] = Arrays.copyOf(flinkKeys[0], flinkKeys[0].length);
			}

			// duplicate self join input to treat it like a regular join
			joinInputs = new ArrayList<DataSet<Tuple>>(numJoinInputs);
			for(int i=0; i<numJoinInputs; i++) {
				joinInputs.add(inputs.get(0));
			}
		}

		if(joiner instanceof InnerJoin) {
			if(!keyFields[0].isNone()) {
				// inner join with keys
//				return prepareInnerCoGroupInput(joinInputs, firstInputKeyFields, flinkKeys, dop); // tests expect buffer being called for empty groups :-/
				return prepareFullOuterCoGroupInput(joinInputs, inputFields, keyFields, flinkKeys, dop);
			}
			else {
				// Cartesian product
				return prepareInnerCrossInput(joinInputs, dop);
			}
		}
		else if(joiner instanceof BufferJoin) {
			return prepareBufferCoGroupInput(joinInputs, inputFields, keyFields, flinkKeys, dop);
		}
		else {
			return prepareFullOuterCoGroupInput(joinInputs, inputFields, keyFields, flinkKeys, dop);
		}

	}

	private DataSet<Tuple2<Tuple, Tuple[]>> prepareInnerCoGroupInput(List<DataSet<Tuple>> inputs,
						Fields[] inputFields, Fields[] keyFields, String[][] flinkKeys, int dop) {

		int numJoinInputs = inputs.size();

		int[] keyPos = inputFields[0].getPos(keyFields[0]);

		TupleTypeInfo keysTypeInfo = new TupleTypeInfo(inputFields[0].select(keyFields[0]));
		keysTypeInfo.registerKeyFields(keyFields[0]);

		TypeInformation<Tuple2<Tuple, Tuple[]>> tupleJoinListsTypeInfo =
				new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple2<Tuple, Tuple[]>>(
						keysTypeInfo,
						ObjectArrayTypeInfo.getInfoFor(Tuple[].class)
				);

		// prepare tuple list for join
		DataSet<Tuple2<Tuple, Tuple[]>> tupleJoinLists = inputs.get(0)
				.mapPartition(new JoinPrepareMapper(numJoinInputs, keyPos))
				.returns(tupleJoinListsTypeInfo);


		for(int i=0; i<flinkKeys[0].length; i++) {
			flinkKeys[0][i] = "f0."+i;
		}

		// inner join inputs
		for (int i = 1; i < numJoinInputs; i++) {
			tupleJoinLists = tupleJoinLists.join(inputs.get(i), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
					.where(flinkKeys[0]).equalTo(flinkKeys[i])
					.with(new TupleAppendJoiner(i))
					.returns(tupleJoinListsTypeInfo)
					.withForwardedFieldsFirst(flinkKeys[0])
					.setParallelism(dop);
		}

		return tupleJoinLists;

	}

	private DataSet<Tuple2<Tuple, Tuple[]>> prepareFullOuterCoGroupInput(List<DataSet<Tuple>> inputs,
						Fields[] inputFields, Fields[] keyFields, String[][] flinkKeys, int dop) {

		int numJoinInputs = inputs.size();

		int[] keyPos = inputFields[0].getPos(keyFields[0]);

		TupleTypeInfo keysTypeInfo = new TupleTypeInfo(inputFields[0].select(keyFields[0]));
		keysTypeInfo.registerKeyFields(keyFields[0]);

		TypeInformation<Tuple2<Tuple, Tuple[]>> tupleJoinListsTypeInfo =
				new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple2<Tuple, Tuple[]>>(
						keysTypeInfo,
						ObjectArrayTypeInfo.getInfoFor(Tuple[].class)
				);

		// prepare tuple list for join
		DataSet<Tuple2<Tuple, Tuple[]>> tupleJoinLists = inputs.get(0)
				.mapPartition(new JoinPrepareMapper(numJoinInputs, 0, keyPos))
				.returns(tupleJoinListsTypeInfo);


		for(int i=0; i<flinkKeys[0].length; i++) {
			flinkKeys[0][i] = "f0."+i;
		}

		// outer join inputs with CoGroup
		for (int i = 1; i < inputs.size(); i++) {
			tupleJoinLists = tupleJoinLists.coGroup(inputs.get(i))
					.where(flinkKeys[0]).equalTo(flinkKeys[i])
					.with(new TupleAppendCoGrouper(i, numJoinInputs, keyPos))
					.returns(tupleJoinListsTypeInfo)
					.withForwardedFieldsFirst(flinkKeys[0])
					.setParallelism(dop);
		}

		return tupleJoinLists;

	}

	private DataSet<Tuple3<Tuple, Integer, Tuple>> prepareBufferCoGroupInput(List<DataSet<Tuple>> inputs,
						Fields[] inputFields, Fields[] keyFields, String[][] flinkKeys, int dop) {

		DataSet<Tuple3<Tuple, Integer, Tuple>> coGroupInput = null;

		for(int i=0; i<inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);

			// get keys
			int[] keyPos = inputFields[i].getPos(keyFields[i]);

			if(keyFields[i].isNone()) {
				// set default key
				keyFields[i] = new Fields("defaultKey");
			}

			TypeInformation<Tuple3<Tuple, Integer, Tuple>> keyedType =
					new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple3<Tuple, Integer, Tuple>>(
							new TupleTypeInfo(inputFields[i].select(keyFields[i])),
							BasicTypeInfo.INT_TYPE_INFO,
							new TupleTypeInfo(inputFields[i])
			);

			// add mapper
			DataSet<Tuple3<Tuple, Integer, Tuple>> keyedInput = input
					.map(new BufferJoinKeyExtractor(i, keyPos))
					.returns(keyedType);

			// add to groupByInput
			if(coGroupInput == null) {
				coGroupInput = keyedInput;
			}
			else {
				coGroupInput = coGroupInput
						.union(keyedInput);
			}
		}

		return coGroupInput;
	}

	private DataSet<Tuple> translateCoGroup(DataSet<?> input, FlowNode node, int dop) {

		CoGroup coGroup = (CoGroup)getSingle(node.getSourceElements());
		int numJoinInputs = coGroup.isSelfJoin() ? coGroup.getNumSelfJoins() + 1 : node.getPreviousScopes(coGroup).size();

		// get out fields of node
		Scope outScope = getOutScope(node);
		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		// get key and value fields of inputs
		List<Scope> inScopes = getInputScopes(node, coGroup);
		Fields keyFields = coGroup.getKeySelectors().get(inScopes.get(0).getName());

		Joiner joiner = coGroup.getJoiner();

		if(!(joiner instanceof BufferJoin)) {
			if (keyFields != Fields.NONE) {

				String[] groupingKeys = new String[keyFields.size()];
				for (int i = 0; i < groupingKeys.length; i++) {
					groupingKeys[i] = "f0." + i;
				}

				DataSet<Tuple> joinResult = ((DataSet<Tuple2<Tuple, Tuple[]>>) input)
						.groupBy(groupingKeys)
						.reduceGroup(new CoGroupReducer(node))
						.withParameters(this.getFlinkNodeConfig(node))
						.setParallelism(dop)
						.returns(new TupleTypeInfo(outFields))
						.name("cogroup-" + node.getID());

				return joinResult;
			} else {
				DataSet<Tuple> joinResult = ((DataSet<Tuple2<Tuple, Tuple[]>>) input)
						.reduceGroup(new CoGroupReducer(node))
						.withParameters(this.getFlinkNodeConfig(node))
						.setParallelism(1)
						.returns(new TupleTypeInfo(outFields))
						.name("cogroup-" + node.getID());

				return joinResult;
			}
		}
		else {
			// Buffer Join
			if (keyFields != Fields.NONE) {

				return ((DataSet<Tuple3<Tuple, Integer, Tuple>>) input)
						.groupBy("f0.*")
						.sortGroup(1, Order.DESCENDING)
						.reduceGroup(new CoGroupBufferReducer(node))
						.withParameters(this.getFlinkNodeConfig(node))
						.setParallelism(dop)
						.returns(new TupleTypeInfo(outFields))
						.name("coGroup-" + node.getID());
			}
			else {
				return ((DataSet<Tuple3<Tuple, Integer, Tuple>>) input)
						.sortPartition(1, Order.DESCENDING)
						.setParallelism(1)
						.reduceGroup(new CoGroupBufferReducer(node))
						.withParameters(this.getFlinkNodeConfig(node))
						.setParallelism(1)
						.returns(new TupleTypeInfo(outFields))
						.name("coGroup-" + node.getID());
			}
		}

	}

	private DataSet<Tuple2<Tuple, Tuple[]>> prepareHashJoinInput(List<DataSet<Tuple>> inputs, FlowNode node, int dop) {

		HashJoin hashJoin = (HashJoin) getCommonSuccessor(node.getSourceElements(), node);
		List<Scope> inScopes = getInputScopes(node, hashJoin);

		Joiner joiner = hashJoin.getJoiner();

		int numJoinInputs = hashJoin.isSelfJoin() ? hashJoin.getNumSelfJoins() + 1 : inputs.size();
		Fields[] keyFields = new Fields[numJoinInputs];
		String[][] flinkKeys = new String[numJoinInputs][];
		List<DataSet<Tuple>> joinInputs;

		// collect key and value fields of inputs
		if(!hashJoin.isSelfJoin()) {
			// regular join with different inputs

			for (int i = 0; i < numJoinInputs; i++) {
				// get input scope
				Scope inScope = inScopes.get(i);

				// get join key fields
				keyFields[i] = hashJoin.getKeySelectors().get(inScope.getName());
				flinkKeys[i] = registerKeyFields(inputs.get(i), keyFields[i]);
			}

			joinInputs = inputs;
		}
		else {
			// self join

			Scope inScope = inScopes.get(0);
			// get join key fields
			keyFields[0] = hashJoin.getKeySelectors().get(inScope.getName());
			flinkKeys[0] = registerKeyFields(inputs.get(0), keyFields[0]);

			for (int i = 1; i < numJoinInputs; i++) {
				keyFields[i] = keyFields[0];
				flinkKeys[i] = Arrays.copyOf(flinkKeys[0], flinkKeys[0].length);
			}

			// duplicate self join input to treat it like a regular join
			joinInputs = new ArrayList<DataSet<Tuple>>(numJoinInputs);
			for(int i=0; i<numJoinInputs; i++) {
				joinInputs.add(inputs.get(0));
			}
		}

		if(joiner instanceof InnerJoin) {
			if(!keyFields[0].isNone()) {
				// inner join with keys
				return prepareInnerHashJoinInput(joinInputs, keyFields, flinkKeys, dop);
			}
			else {
				// Cartesian product
				return prepareInnerCrossInput(joinInputs, dop);
			}
		}
		else {
			throw new FlowException("HashJoin does only support InnerJoin.");
		}

	}

	private DataSet<Tuple2<Tuple, Tuple[]>> prepareInnerHashJoinInput(List<DataSet<Tuple>> inputs, Fields[] keyFields, String[][] flinkKeys, int dop) {

		int numJoinInputs = inputs.size();

		Fields inputFields = ((TupleTypeInfo)inputs.get(0).getType()).getFields();
		int[] keyPos = inputFields.getPos(keyFields[0]);

		TupleTypeInfo keysTypeInfo = new TupleTypeInfo(inputFields.select(keyFields[0]));
		keysTypeInfo.registerKeyFields(keyFields[0]);

		TypeInformation<Tuple2<Tuple, Tuple[]>> tupleJoinListsTypeInfo =
				new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple2<Tuple, Tuple[]>>(
						keysTypeInfo,
						ObjectArrayTypeInfo.getInfoFor(Tuple[].class)
				);

		// prepare tuple list for join
		DataSet<Tuple2<Tuple, Tuple[]>> tupleJoinLists = inputs.get(0)
				.mapPartition(new JoinPrepareMapper(numJoinInputs, keyPos))
				.returns(tupleJoinListsTypeInfo);

		for(int i=0; i<flinkKeys[0].length; i++) {
			flinkKeys[0][i] = "f0."+i;
		}

		// regular inner join
		for (int i = 1; i < inputs.size(); i++) {
			tupleJoinLists = tupleJoinLists.join(inputs.get(i), JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
					.where(flinkKeys[0]).equalTo(flinkKeys[i])
					.with(new TupleAppendJoiner(i))
					.returns(tupleJoinListsTypeInfo)
					.withForwardedFieldsFirst(flinkKeys[0])
					.setParallelism(dop);
		}

		return tupleJoinLists;
	}

	private DataSet<Tuple2<Tuple, Tuple[]>> prepareInnerCrossInput(List<DataSet<Tuple>> inputs, int dop) {

		int numJoinInputs = inputs.size();

		TypeInformation<Tuple2<Tuple, Tuple[]>> tupleJoinListsTypeInfo =
				new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple2<Tuple, Tuple[]>>(
						new TupleTypeInfo(Fields.UNKNOWN),
						ObjectArrayTypeInfo.getInfoFor(Tuple[].class)
				);

		// prepare tuple list for join
		DataSet<Tuple2<Tuple, Tuple[]>> tupleJoinLists = inputs.get(0)
				.mapPartition(new JoinPrepareMapper(numJoinInputs, null))
				.returns(tupleJoinListsTypeInfo);

		for (int i = 1; i < inputs.size(); i++) {
			tupleJoinLists = tupleJoinLists.crossWithTiny(inputs.get(i))
					.with(new TupleAppendCrosser(i))
					.returns(tupleJoinListsTypeInfo)
					.setParallelism(dop);
		}

		return tupleJoinLists;
	}


	private DataSet<Tuple> translateHashJoin(List<DataSet<Tuple>> inputs, FlowNode node, int dop) {

		DataSet<Tuple2<Tuple, Tuple[]>> input = prepareHashJoinInput(inputs, node, dop);

		// get out fields of node
		Scope outScope = getOutScope(node);
		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		DataSet<Tuple> joinResult = input
				.mapPartition(new HashJoinMapper(node))
				.withParameters(this.getFlinkNodeConfig(node))
				.setParallelism(dop)
				.returns(new TupleTypeInfo(outFields))
				.name("hashjoin-" + node.getID());

		return joinResult;

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

	private Scope getOutScope(FlowNode node) {

		Set<FlowElement> nodeSinks = node.getSinkElements();
		if(nodeSinks.size() != 1) {
			throw new RuntimeException("Only nodes with one output supported right now");
		}
		FlowElement sink =  nodeSinks.iterator().next();

		Collection<Scope> outScopes = (Collection<Scope>) node.getPreviousScopes(sink);
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

	private FlowElement getCommonSuccessor(Set<FlowElement> set, FlowNode node) {

		ElementGraph graph = node.getElementGraph();
		FlowElement successor = null;
		for(FlowElement e : set) {
			List<FlowElement> successors = graph.successorListOf(e);
			if(successors.size() > 1) {
				return null;
			}
			else {
				if(successor == null) {
					successor = successors.get(0);
				}
				else if(successor != successors.get(0)){
					return null;
				}
			}
		}
		return successor;
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

	private org.apache.flink.configuration.Configuration getFlinkNodeConfig(FlowNode node) {
		return FlinkConfigConverter.toFlinkConfig(this.getNodeConfig(node));
	}

	private Configuration getNodeConfig(FlowNode node) {

		Configuration nodeConfig = HadoopUtil.copyConfiguration(this.getConfig());
		ConfigurationSetter configSetter = new ConfigurationSetter(nodeConfig);
		this.initConfFromNodeConfigDef(node.getElementGraph(), configSetter);
		this.initConfFromStepConfigDef(configSetter);

		return nodeConfig;
	}

	private static class ConfigurationSetter implements ConfigDef.Setter
	{
		private final Configuration conf;

		public ConfigurationSetter( Configuration conf )
		{
			this.conf = conf;
		}

		@Override
		public String set( String key, String value ) {
			String oldValue = get( key );
			conf.set( key, value );

			return oldValue;
		}

		@Override
		public String update( String key, String value ) {
			String oldValue = get( key );

			if( oldValue == null ) {
				conf.set(key, value);
			}
			else if( !oldValue.contains( value ) ) {
				conf.set(key, oldValue + "," + value);
			}

			return oldValue;
		}

		@Override
		public String get( String key ) {
			String value = conf.get( key );

			if( value == null || value.isEmpty() ) {
				return null;
			}

			return value;
		}
	}

}
