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
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.dataArtisans.flinkCascading.exec.operators.HfsOutputFormat;
import com.dataArtisans.flinkCascading.planning.translation.AggregatorOperator;
import com.dataArtisans.flinkCascading.planning.translation.BufferOperator;
import com.dataArtisans.flinkCascading.planning.translation.DataSource;
import com.dataArtisans.flinkCascading.planning.translation.EachOperator;
import com.dataArtisans.flinkCascading.planning.translation.Operator;
import com.dataArtisans.flinkCascading.planning.translation.PipeOperator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

	// TODO: REWRITE TO A GRAPH OF FLINK OPERATOR NODES:
	// 1. check flow, choose which pipes to process in one operator, and build a graph from that
	// 2. translate the graph as a second step

	@Override
	public FlinkFlow buildFlow( FlowDef flow, RuleRegistrySet ruleRegistrySet ) {

		Map<FlowElement, Operator> memo =
				new HashMap<FlowElement, Operator>();

		Pipe[] tailsA = flow.getTailsArray();
		FlowElementGraph flowGraph = createFlowElementGraph(flow, tailsA);
		flowGraph.resolveFields();

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
			else if (e instanceof Tap && sources.contains(e)) {

				Set<Scope> outScopes = flowGraph.outgoingEdgesOf(e);

				DataSource source = new DataSource((Tap)e, getSingleScope(outScopes));
				memo.put(e, source);
			}
			else if (e instanceof Hfs && sinks.contains(e)) {
				// do nothing
			}
			else if (e instanceof Each) {

				Each each = (Each)e;
				Operator inOp = getInputOp(each, flowGraph, memo);
				Set<Scope> inScopes = flowGraph.incomingEdgesOf(e);
				Set<Scope> outScopes = flowGraph.outgoingEdgesOf(e);

				EachOperator eachOp = new EachOperator(each, getSingleScope(inScopes), getSingleScope(outScopes), inOp );

				memo.put(e, eachOp);
			}
			else if (e instanceof GroupBy) {
				GroupBy groupBy = (GroupBy) e;
				Set<Scope> groupByInScopes = flowGraph.incomingEdgesOf(e);
				Set<Scope> groupByOutScopes = flowGraph.outgoingEdgesOf(e);
				List<Operator> inOps = getInputOps(groupBy, flowGraph, memo);

				FlowElement groupOperation = it.next();

				if(groupOperation instanceof Every) {

					Every every = (Every)groupOperation;
					Set<Scope> everyInScopes = flowGraph.incomingEdgesOf(every);
					Set<Scope> everyOutScopes = flowGraph.outgoingEdgesOf(every);

					if(every.isAggregator()) {

						// TODO: handle multi input correctly. Get inscopes and ops in the same order
						AggregatorOperator aggOp = new AggregatorOperator(groupBy, every,
								Collections.singletonList(getSingleScope(groupByInScopes)), getSingleScope(groupByOutScopes),
								getSingleScope(everyInScopes), getSingleScope(everyOutScopes), inOps);

						memo.put(every, aggOp);

					}
					else if(every.isBuffer()) {

						BufferOperator bufOp = new BufferOperator(groupBy, every,
								Collections.singletonList(getSingleScope(groupByInScopes)), getSingleScope(groupByOutScopes),
								getSingleScope(everyInScopes), getSingleScope(everyOutScopes), inOps);

						memo.put(every, bufOp);

					}
					else if(every.isGroupAssertion()) {
						throw new RuntimeException("GroupAssertion not yet supported");
					}
					else {
						throw new RuntimeException("Unknown Every type");
					}

				}

			}
			else if (e instanceof Every) {
				Every every = (Every) e;
				Set<Scope> inScopes = flowGraph.incomingEdgesOf(every);
				Set<Scope> outScopes = flowGraph.outgoingEdgesOf(every);

				if(every.isAggregator()) {
					// check if we can append to existing aggregation

					List<Operator> inputOps = getInputOps(every, flowGraph, memo);
					if(inputOps.size() != 1) {
						throw new RuntimeException("Every accepts only a single input.");
					}
					Operator inputOp = inputOps.get(0);
					if(!(inputOp instanceof AggregatorOperator)) {
						// TODO: can also be a Every after a CoGroup...
						throw new RuntimeException("Aggregation Every can only be appended to other Aggregations");
					}
					((AggregatorOperator) inputOp).addAggregator(every, getSingleScope(inScopes), getSingleScope(outScopes));

					// add to memo
					memo.put(every, inputOp);

				}
				else if(every.isBuffer()) {
					throw new RuntimeException("Buffer without grouping not possible");
					// TODO: check for CoGroup...
				}
				else {
					throw new RuntimeException("Can not handle abandoned Every.");
				}

			}
			else if (e instanceof Merge) {

				throw new RuntimeException("Merge not yet supported...");

//				Merge merge = (Merge) e;
//				Set<Scope> inScopes = flowGraph.incomingEdgesOf(e);
//				Set<Scope> outScopes = flowGraph.outgoingEdgesOf(e);
//				List<Operator> inOps = getInputOps(merge, flowGraph, memo);
//
//				UnionOperator unionOp = new UnionOperator(inOps, );
//
//				memo.put(merge, unionOp);
			}
			else if (e instanceof Pipe) {
				// must stay last because it is super-class
				Pipe pipe = (Pipe) e;
				Set<Scope> inScopes = flowGraph.incomingEdgesOf(e);
				Set<Scope> outScopes = flowGraph.outgoingEdgesOf(e);
				Operator inOp = getInputOp(pipe, flowGraph, memo);

				PipeOperator pipeOp = new PipeOperator(pipe, getSingleScope(inScopes), getSingleScope(outScopes), inOp );

				memo.put(pipe, pipeOp);
			} else {
				throw new UnsupportedOperationException("Unknown FlowElement");
			}
		}

		Map<String, Tap> sinkMap = flowGraph.getSinkMap();
		for(FlowElement tail : tails) {
			Operator tailOp = memo.get(tail);
			DataSet flinkTail = tailOp.getFlinkOperator(env);
			attachSink(flinkTail, (Pipe) tail, sinkMap);
		}

		return new FlinkFlow(env);

	}


	private Operator getInputOp(Pipe pipe, FlowElementGraph flowGraph, Map<FlowElement, Operator> memo) {

		List<Operator> inputOps = getInputOps(pipe, flowGraph, memo);

		if(inputOps.size() > 1) {
			throw new RuntimeException("Operator with a single input has multiple inputs.");
		}
		else {
			return inputOps.get(0);
		}

	}

	private List<Operator> getInputOps(Pipe pipe, FlowElementGraph flowGraph, Map<FlowElement, Operator> memo) {

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

		List<Operator> inputOps = new ArrayList<Operator>();
		for(FlowElement e : inputs) {
			Operator op = memo.get(e);
			if(op == null) {
				throw new RuntimeException("Could not find flink operator for input flow element.");
			}
			inputOps.add(memo.get(e));
		}

		return inputOps;

	}


	private void attachSink(DataSet tail, Pipe p, Map<String, Tap> sinkMap) {

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

	private Scope getSingleScope(Set<Scope> scopes) {
		if(scopes.size() != 1) {
			throw new RuntimeException("Not exactly one scope.");
		}
		return scopes.iterator().next();
	}


}
