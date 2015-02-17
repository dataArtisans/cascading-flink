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

import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.EachFunctionMapper;
import com.dataArtisans.flinkCascading.exec.operators.EveryReducer;
import com.dataArtisans.flinkCascading.exec.operators.HfsInputFormat;
import com.dataArtisans.flinkCascading.exec.operators.HfsOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.KeyExtractor;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.hadoop.conf.Configuration;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class FlinkFlowPlanner extends FlowPlanner<FlinkFlow, Configuration> {

	public ExecutionEnvironment env;

	private static boolean PRINT_STDOUT = true;

	public FlinkFlowPlanner(ExecutionEnvironment env) {
		this.env = env;
	}

	@Override
	public Configuration getDefaultConfig() {
		return null;
	}

	@Override
	public PlatformInfo getPlatformInfo() {

		return new PlatformInfo("Apache Flink", "data Artisans GmbH", "0.1");
	}

	@Override
	protected FlinkFlow createFlow(FlowDef flowDef) {

		return new FlinkFlow(env);
	}

	@Override
	public FlowStep<Configuration> createFlowStep(ElementGraph elementGraph, FlowNodeGraph flowNodeGraph) {
		return null;
	}

	@Override
	protected Tap makeTempTap(String s, String s1) {
		return null;
	}

	@Override
	public FlinkFlow buildFlow( FlowDef flow, RuleRegistrySet ruleRegistrySet ) {

		Map<FlowElement, IntermediateProgram> memo =
				new HashMap<FlowElement, IntermediateProgram>();

		Pipe[] tailsA = flow.getTailsArray();
		FlowElementGraph flowGraph = createFlowElementGraph(flow, tailsA);

		TopologicalOrderIterator<FlowElement, Scope> it = flowGraph.getTopologicalIterator();
		Collection<Tap> sources = flowGraph.getSources();
		Collection<Tap> sinks = flowGraph.getSinks();
		Set<FlowElement> tails = new HashSet<FlowElement>();
		for(Pipe t : tailsA) {
			tails.add(t);
		}

		while (it.hasNext()) {

			FlowElement e = it.next();

			if (memo.containsKey(e)) {
				// we have been here before
				continue;
			}

			if (e instanceof Extent) {
				// to nothing
			}
			else if (e instanceof Hfs && sources.contains(e)) {

				memo.put(e, translateHfs((Hfs)e));
			}
			else if (e instanceof Hfs && sinks.contains(e)) {
				// do nothing
			}
			else if (e instanceof Each) {
				Each each = (Each) e;

				memo.put(e, translateEach(each, memo, flowGraph));
				appendSinkIfTail(each, memo, tails, flowGraph.getSinkMap());

			}
			else if (e instanceof GroupBy) {
				GroupBy groupBy = (GroupBy) e;

				memo.put(e, translateGroupBy(groupBy, memo, flowGraph));
			}
			else if (e instanceof Every) {
				Every every = (Every) e;

				memo.put(e, translateEvery(every, memo, flowGraph));
				appendSinkIfTail(every, memo, tails, flowGraph.getSinkMap());

			}
			else if (e instanceof Pipe) {
				// must stay last because it is super-class
				Pipe pipe = (Pipe) e;

				memo.put(e, translatePipe(pipe, memo, flowGraph));
			} else {
				throw new UnsupportedOperationException("Unknown FlowElement");
			}
		}

		return new FlinkFlow(env);

	}

	private IntermediateProgram translateHfs(Hfs hfs) {

		Configuration conf = new Configuration();
		hfs.getScheme().sourceConfInit(null, hfs, conf);
		conf.set("mapreduce.input.fileinputformat.inputdir", hfs.getPath().toString());

		DataSet<Tuple> src = env.createInput(new HfsInputFormat(hfs, conf));
		Scope outScope = hfs.outgoingScopeFor(Collections.singleton(new Scope()));

		return new IntermediateProgram(src, outScope);
	}

	private IntermediateProgram translateEach(Each each, Map<FlowElement, IntermediateProgram> memo, FlowElementGraph flowGraph) {

		FlowElement[] prevs = getPrevious(each, flowGraph);
		FlowElement prev;

		if(prevs == null) {
			throw new RuntimeException("Could not find input of Each");
		} else if (prevs.length == 1) {
			prev = prevs[0];
		} else {
			// TODO: check if this should be allowed
			throw new RuntimeException("Multi-input not supported, yet");
		}

		IntermediateProgram input = memo.get(prev);
		if (input == null) {
			throw new RuntimeException("Input was not translated!");
		}

		Scope inScope = input.getOutgoing();
		Scope outScope = each.outgoingScopeFor(Collections.singleton(inScope));

		MapPartitionFunction mapFunc;

		if (each.isFunction()) {
			mapFunc = new EachFunctionMapper(each, inScope, outScope);
		}
		else if (each.isFilter()) {
			throw new UnsupportedOperationException("Filter not supported yet!");
		}
		else if (each.isValueAssertion()) {
			throw new UnsupportedOperationException("ValueAssertion not supported yet!");
		}
		else {
			throw new UnsupportedOperationException("Unsupported Each!");
		}
		DataSet mapped = input.getDataSet()
				.mapPartition(mapFunc)
				.name(each.getName());

		return new IntermediateProgram(mapped, outScope);

	}


	private IntermediateProgram translateEvery(Every every, Map<FlowElement, IntermediateProgram> memo, FlowElementGraph flowGraph) {

		FlowElement[] prevs = getPrevious(every, flowGraph);
		FlowElement prev;

		if (prevs == null) {
			throw new RuntimeException("Could not find input of Each");
		} else if (prevs.length == 1) {
			prev = prevs[0];
		} else {
			throw new RuntimeException("Multi-input not supported, yet");
		}

		IntermediateProgram input = memo.get(prev);
		if (input == null) {
			throw new RuntimeException("Input was not translated!");
		}

		Scope inScope = input.getOutgoing();
		Scope outScope = every.outgoingScopeFor(Collections.singleton(inScope));

		if (!input.isGrouping()) {
			throw new RuntimeException("Input is not a grouping");
		}

		GroupReduceFunction reduceFunc;

		if (every.isAggregator()) {
			reduceFunc = new EveryReducer(every, inScope, outScope);
		}
		else if (every.isBuffer()) {
			throw new RuntimeException("Buffer not supported, yet");
		}
		else if (every.isGroupAssertion()) {
			throw new RuntimeException("GroupAssertion not supported, yet");
		}

		// add reduce function

		Grouping grouping = input.getGrouping();
		DataSet reduced;
		if (grouping instanceof UnsortedGrouping) {
			reduced = ((UnsortedGrouping) grouping)
					.reduceGroup(new EveryReducer(every, inScope, outScope))
					.name(every.getName());
		} else if (grouping instanceof SortedGrouping) {
			reduced = ((SortedGrouping) grouping)
					.reduceGroup(new EveryReducer(every, inScope, outScope))
					.name(every.getName());
		} else {
			throw new RuntimeException("Unknown grouping encountered!");
		}

		return new IntermediateProgram(reduced, outScope);
	}

	private IntermediateProgram translateGroupBy(GroupBy groupBy, Map<FlowElement, IntermediateProgram> memo, FlowElementGraph flowGraph) {

		FlowElement[] prevs = getPrevious(groupBy, flowGraph);
		FlowElement prev;

		Scope inScope;
		DataSet input;

		if(prevs == null) {
			throw new RuntimeException("Could not find input of Each");
		} else if (prevs.length == 1) {

			IntermediateProgram i = memo.get(prevs[0]);
			if (i == null) {
				throw new RuntimeException("Input was not translated!");
			}

//			inScope = i.getOutgoing();
			inScope = new Scope(i.getOutgoing()); // TODO
			inScope.setName("wc"); // TODO

			input = i.getDataSet();

		} else {

			IntermediateProgram i = memo.get(prevs[0]);
			if (i == null) {
				throw new RuntimeException("Input was not translated!");
			}

			// TODO: we assume all input have the same schema. Need to check that probably
//			inScope = i.getOutgoing();
			inScope = new Scope(i.getOutgoing()); // TODO
			inScope.setName("wc"); // TODO

			input = i.getDataSet();
			for(int j=1; j<prevs.length; j++) {

				i = memo.get(prevs[j]);
				if(i == null) {
					throw new RuntimeException("Input was not translated!");
				}

				input = input.union(i.getDataSet());
			}
		}

		Scope outScope = groupBy.outgoingScopeFor(Collections.singleton(inScope));

		Map<String, Fields> groupingKeys = groupBy.getKeySelectors();
		Map<String, Fields> sortingKeys = groupBy.getSortingSelectors();

		// TODO
		Grouping grouping = input.map(
				new KeyExtractor(groupingKeys.get("wc"), groupingKeys.get("wc"), sortingKeys.get("wc")))
				.groupBy(0);

		return new IntermediateProgram(grouping, outScope);

	}

	private IntermediateProgram translatePipe(Pipe pipe, Map<FlowElement, IntermediateProgram> memo, FlowElementGraph flowGraph) {

		FlowElement[] prevs = getPrevious(pipe, flowGraph);
		FlowElement prev;

		if(prevs == null) {
			throw new RuntimeException("Could not find input of Each");
		} else if (prevs.length == 1) {
			prev = prevs[0];
		} else {
			// TODO: check if this is possible
			throw new RuntimeException("Multi-input not supported, yet");
		}

		IntermediateProgram input = memo.get(prev);
		if (input == null) {
			throw new RuntimeException("Input was not translated!");
		}

		Scope inScope = input.getOutgoing();
		Scope outScope = pipe.outgoingScopeFor(Collections.singleton(inScope));

		if (input.isGrouping()) {
			// TODO: check if this is possible
			return new IntermediateProgram(input.getGrouping(), outScope);
		} else {
			return new IntermediateProgram(input.getDataSet(), outScope);
		}

	}

	private FlowElement[] getPrevious(Pipe pipe, FlowElementGraph flowGraph) {

		FlowElement[] inputs = pipe.getPrevious();
		if(inputs == null || inputs.length == 0) {
			// try to get source
			FlowElement source = flowGraph.getSourceMap().get(pipe.getName());
			if(source != null) {
				inputs = new FlowElement[]{source};
			} else {
				return null;
			}
		}
		return inputs;

	}

	private void appendSinkIfTail(Pipe p, Map<FlowElement, IntermediateProgram> memo, Set<FlowElement> tails, Map<String, Tap> sinkMap) {
		if(tails.contains(p)) {

			DataSet tail = memo.get(p).getDataSet();
			Tap sink = sinkMap.get(p.getName());

			if(PRINT_STDOUT) {
				tail.print();
				return;
			} else {

				if (sink instanceof Hfs) {

					Hfs hfs = (Hfs) sink;
					Configuration conf = new Configuration();

					tail
							.output(new HfsOutputFormat(hfs, conf))
							.setParallelism(1);
				} else {
					throw new RuntimeException("Unsupported Tap");
				}
			}

		}
	}


	private static class IntermediateProgram {

		private DataSet dataSet;
		private Grouping grouping;

		private Scope outgoing;

		public IntermediateProgram(DataSet dataSet, Scope outgoing) {
			this.dataSet = dataSet;
			this.outgoing = outgoing;
		}

		public IntermediateProgram(Grouping grouping, Scope outgoing) {
			this.grouping = grouping;
			this.outgoing = outgoing;
		}

		public boolean isGrouping() {
			return this.grouping != null;
		}

		public DataSet getDataSet() {
			return dataSet;
		}

		public Grouping getGrouping() {
			return grouping;
		}

		public Scope getOutgoing() {
			return outgoing;
		}
	}


}
