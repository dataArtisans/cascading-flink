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

package com.dataArtisans.flinkCascading.runners;

import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.graph.FlowElementGraph;
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
import com.dataArtisans.flinkCascading.exec.operators.KeyExtractor;
import com.dataArtisans.flinkCascading.flows.TokenizeFlow;
import com.dataArtisans.flinkCascading.flows.WordCountFlow;
import com.dataArtisans.flinkCascading.planning.FlinkFlow;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.Collections;
import java.util.Map;

public class FlinkFlowRunner {

	public static void main(String[] args) throws Exception {

		FlowDef tokenizeFlow = TokenizeFlow.getTokenizeFlow(
				"file:///users/fhueske/testFile",
				"file:///users/fhueske/wcResult");

		FlowDef wcFlow = WordCountFlow.getWordCountFlow(
				"file:///users/fhueske/testFile",
				"file:///users/fhueske/wcResult");



		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple> prog = getFlinkProg(env, wcFlow);
		prog.print();

		System.out.println(env.getExecutionPlan());

		env.execute();

	}


	private static DataSet<Tuple> getFlinkProg(ExecutionEnvironment env, FlowDef flowDef) {

		FlinkFlow flow =  new FlinkFlow(env); // ( tokenizeFlow );
		Pipe[] tails = flowDef.getTailsArray();
		FlowElementGraph flowElementGraph = createFlowElementGraph( flowDef, tails );

		TopologicalOrderIterator<FlowElement, Scope> it = flowElementGraph.getTopologicalIterator();

		DataSet<Tuple> cur = null;
		Grouping<Tuple3<Tuple,Tuple,Tuple>> curGrouping = null;

		Scope prev = null;

		while(it.hasNext()) {

			FlowElement e = it.next();

			if(e instanceof Extent) {
				// to nothing
			}
			else if (e instanceof Hfs && flowElementGraph.getSources().contains(e)) {

				Hfs hfs = (Hfs)e;

				Configuration conf = new Configuration();
				hfs.getScheme().sourceConfInit(null, hfs, conf);
				conf.set("mapreduce.input.fileinputformat.inputdir", hfs.getPath().toString());

				cur = env.createInput(new HfsInputFormat((Hfs) e, conf));
				prev = hfs.outgoingScopeFor(Collections.singleton(new Scope()));
			}
			else if (e instanceof Hfs && flowElementGraph.getSinks().contains(e)) {
				// not handling sinks yet
				System.out.println(e);
			}
			else if(e instanceof Each) {
				Each each = (Each) e;
				Scope inScope = prev;
				Scope outScope = each.outgoingScopeFor(Collections.singleton(inScope));

				if (each.isFunction()) {
					cur = cur.mapPartition(new EachFunctionMapper(each, inScope, outScope));
				} else {
					throw new UnsupportedOperationException("Unknown each!");
				}
				prev = outScope;
			}
			else if(e instanceof GroupBy) {
				GroupBy groupBy = (GroupBy)e;

				Scope inScope = new Scope(prev);
				inScope.setName("wc");
				Scope outScope = groupBy.outgoingScopeFor(Collections.singleton(inScope));

				Map<String, Fields> groupingKeys = groupBy.getKeySelectors();
				Map<String, Fields> sortingKeys = groupBy.getSortingSelectors();

				curGrouping = cur.map(new KeyExtractor(groupingKeys.get("wc"), groupingKeys.get("wc"), sortingKeys.get("wc")))
						.groupBy(0);

				prev = outScope;
			}
			else if(e instanceof Every) {
				Every every = (Every) e;
				Scope inScope = prev;
				Scope outScope = every.outgoingScopeFor(Collections.singleton(inScope));

				if(curGrouping == null) {
					throw new RuntimeException("No open grouping found");
				}

				cur = ((UnsortedGrouping)curGrouping).reduceGroup(new EveryReducer(every, inScope, outScope));

				prev = outScope;

			}
			else if (e instanceof Pipe) {
				// not sure what to do
				Scope inScope = prev;
				Scope outScope = ((Pipe)e).outgoingScopeFor(Collections.singleton(inScope));

				prev = outScope;
			}
			else {
				throw new UnsupportedOperationException("Unknown FlowElement");
			}

			System.out.println(e);
			System.out.println(prev);

		}

		return cur;
	}


	protected static FlowElementGraph createFlowElementGraph( FlowDef flowDef, Pipe[] flowTails )
	{
		Map<String, Tap> sources = flowDef.getSourcesCopy();
		Map<String, Tap> sinks = flowDef.getSinksCopy();
		Map<String, Tap> traps = flowDef.getTrapsCopy();
		Map<String, Tap> checkpoints = flowDef.getCheckpointsCopy();

//		checkpointTapRootPath = makeCheckpointRootPath( flowDef );

		return new FlowElementGraph( new PlatformInfo("Flink","",""), flowTails, sources, sinks, traps, checkpoints, false);
	}

}
