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
import cascading.flow.planner.rule.RuleResult;
import cascading.flow.planner.rule.RuleSetExec;
import cascading.flow.planner.rule.util.TraceWriter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import com.dataArtisans.flinkCascading.planning.translation.DataSink;
import com.dataArtisans.flinkCascading.planning.translation.GroupByOperator;
import com.dataArtisans.flinkCascading.planning.translation.CoGroupOperator;
import com.dataArtisans.flinkCascading.planning.translation.DataSource;
import com.dataArtisans.flinkCascading.planning.translation.EachOperator;
import com.dataArtisans.flinkCascading.planning.translation.MergeOperator;
import com.dataArtisans.flinkCascading.planning.translation.Operator;
import com.dataArtisans.flinkCascading.planning.translation.PipeOperator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FlinkFlowPlanner extends FlowPlanner<FlinkFlow, Configuration> {

	public ExecutionEnvironment env;

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

		return new FlinkFlow(env, this.getPlatformInfo(), flowDef);
	}

	@Override
	public FlowStep<Configuration> createFlowStep(ElementGraph elementGraph, FlowNodeGraph flowNodeGraph) {
		return null;
	}

	@Override
	protected Tap makeTempTap(String s, String s1) {
		return null;
	}

	// TODO: REWRITE TO A GRAPH OF FLINK OPERATOR NODES:
	// 1. check flow, choose which pipes to process in one operator, and build a graph from that
	// 2. translate the graph as a second step

	@Override
	public FlinkFlow buildFlow( FlowDef flowDef, RuleRegistrySet ruleRegistrySet ) {

		Map<FlowElement, Operator> memo =
				new HashMap<FlowElement, Operator>();

		FlinkFlow flow = createFlow(flowDef);

		Pipe[] tailsA = flowDef.getTailsArray();
		FlowElementGraph initFlowGraph = createFlowElementGraph(flowDef, tailsA);

		TraceWriter traceWriter = new TraceWriter( flow );
		RuleSetExec ruleSetExec = new RuleSetExec( traceWriter, this, flow, ruleRegistrySet, flowDef, initFlowGraph );

		RuleResult ruleResult = ruleSetExec.exec();

		FlowElementGraph flowGraph = ruleResult.getAssemblyGraph();

		flowGraph.resolveFields();

		TopologicalOrderIterator<FlowElement, Scope> it = flowGraph.getTopologicalIterator();
		Collection<Tap> sources = flowGraph.getSources();
		Collection<Tap> sinks = flowGraph.getSinks();

		Set<FlowElement> tails = new HashSet<FlowElement>();
		for(Pipe t : tailsA) {
			tails.add(t);
		}

		Set<DataSink> flinkSinks = new HashSet<DataSink>();

		while (it.hasNext()) {

			FlowElement e = it.next();

			if (memo.containsKey(e)) {
				// we have been here before
				continue;
			}

			if (e instanceof Extent) {
				// to nothing
			}
			else if (e instanceof Tap && sources.contains(e)) {

				DataSource source = new DataSource((Tap)e, flowGraph);
				memo.put(e, source);
			}
			else if (e instanceof Tap && sinks.contains(e)) {

				Operator inOp = getInputOp(e, flowGraph, memo);

				flinkSinks.add(new DataSink((Tap)e, inOp, flowGraph));
			}
			else if (e instanceof Each) {

				Each each = (Each)e;
				Operator inOp = getInputOp(each, flowGraph, memo);

				EachOperator eachOp = new EachOperator(each, inOp, flowGraph );
				memo.put(e, eachOp);
			}
			else if (e instanceof GroupBy) {

				GroupBy groupBy = (GroupBy) e;
				List<Operator> inOps = getInputOps(groupBy, flowGraph, memo);

				GroupByOperator groupByOp = new GroupByOperator(groupBy, inOps, flowGraph);
				memo.put(groupBy, groupByOp);
			}
			else if (e instanceof Every) {
				Every every = (Every) e;

				List<Operator> inputOps = getInputOps(every, flowGraph, memo);
				if(inputOps.size() != 1) {
					throw new RuntimeException("Every accepts only a single input.");
				}
				Operator inputOp = inputOps.get(0);

				if(inputOp instanceof GroupByOperator) {

					((GroupByOperator) inputOp).addEvery(every);
					memo.put(every, inputOp);
				}
				else if(inputOp instanceof CoGroupOperator) {

					((CoGroupOperator) inputOp).addEvery(every);
					memo.put(every, inputOp);
				}
				else {
					throw new RuntimeException("Every can only be chained to GroupBy or CoGroup");
				}

			}
			else if (e instanceof Merge) {

				Merge merge = (Merge) e;
				List<Operator> inOps = getInputOps(merge, flowGraph, memo);

				MergeOperator mergeOp = new MergeOperator(merge, inOps, flowGraph);
				memo.put(merge, mergeOp);
			}
			else if (e instanceof CoGroup) {

				CoGroup coGroup = (CoGroup) e;
				List<Operator> inOps = getInputOps(coGroup, flowGraph, memo);

				CoGroupOperator coGroupOp = new CoGroupOperator(coGroup, inOps, flowGraph);
				memo.put(coGroup, coGroupOp);
			}
			else if (e instanceof Pipe) {
				// must stay last because it is super-class
				Pipe pipe = (Pipe) e;
				Operator inOp = getInputOp(pipe, flowGraph, memo);

				PipeOperator pipeOp = new PipeOperator(pipe, inOp, flowGraph );
				memo.put(pipe, pipeOp);

			} else {
				throw new UnsupportedOperationException("Unknown FlowElement");
			}
		}

		for(DataSink s : flinkSinks) {
			s.getFlinkOperator(env);
		}

		return new FlinkFlow(env, getPlatformInfo(), flowDef);

	}


	private Operator getInputOp(FlowElement flowElement, FlowElementGraph flowGraph, Map<FlowElement, Operator> memo) {

		List<Operator> inputOps = getInputOps(flowElement, flowGraph, memo);

		if(inputOps.size() > 1) {
			throw new RuntimeException("Operator with a single input has multiple inputs.");
		}
		else {
			return inputOps.get(0);
		}

	}

	private List<Operator> getInputOps(FlowElement flowElement, FlowElementGraph flowGraph, Map<FlowElement, Operator> memo) {

		Set<Scope> incomingEdges = flowGraph.incomingEdgesOf(flowElement);
		if((incomingEdges == null || incomingEdges.size() == 0)) {
			throw new RuntimeException("Operator does not have inputs.");
		}
		else {
			Operator[] inputOps = new Operator[incomingEdges.size()];

			for (Scope s : incomingEdges) {
				FlowElement e = flowGraph.getEdgeSource(s);
				Operator op = memo.get(e);
				if (op == null) {
					throw new RuntimeException("Could not find flink operator for input flow element.");
				}
				inputOps[s.getOrdinal()] = op;
			}
			return Arrays.asList(inputOps);
		}
	}

}
