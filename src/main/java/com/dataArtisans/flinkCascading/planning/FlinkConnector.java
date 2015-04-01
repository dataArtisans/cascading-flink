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

import cascading.flow.FlowConnector;
import cascading.flow.local.planner.LocalRuleRegistry;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.flow.planner.rule.annotator.BlockingHashJoinAnnotator;
import cascading.flow.planner.rule.annotator.HashJoinBlockingHashJoinAnnotator;
import cascading.flow.planner.rule.assertion.BufferAfterEveryAssert;
import cascading.flow.planner.rule.assertion.EveryAfterBufferAssert;
import cascading.flow.planner.rule.assertion.LoneGroupAssert;
import cascading.flow.planner.rule.assertion.MissingGroupAssert;
import cascading.flow.planner.rule.assertion.SplitBeforeEveryAssert;
import cascading.flow.planner.rule.partitioner.WholeGraphNodePartitioner;
import cascading.flow.planner.rule.partitioner.WholeGraphStepPartitioner;
import cascading.flow.planner.rule.transformer.ApplyAssertionLevelTransformer;
import cascading.flow.planner.rule.transformer.ApplyDebugLevelTransformer;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;
import cascading.scheme.Scheme;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkConnector extends FlowConnector {

	private ExecutionEnvironment env;

	public FlinkConnector(ExecutionEnvironment env) {
		this.env = env;
	}

	@Override
	protected Class<? extends Scheme> getDefaultIntermediateSchemeClass() {
		return null;
	}

	@Override
	protected FlowPlanner createFlowPlanner() {
		return new FlinkFlowPlanner(env);
	}

	@Override
	protected RuleRegistrySet createDefaultRuleRegistrySet() {

		return new RuleRegistrySet(new FlinkRuleRegistry());
	}

	public static class FlinkRuleRegistry extends RuleRegistry
	{
		public FlinkRuleRegistry()
		{
			addRule( new LoneGroupAssert() );
			addRule( new MissingGroupAssert() );

			addRule( new BufferAfterEveryAssert() );
			addRule( new EveryAfterBufferAssert() );
			addRule( new SplitBeforeEveryAssert() );

			addRule( new RemoveNoOpPipeTransformer() );

			addRule( new ApplyAssertionLevelTransformer() );
			addRule( new ApplyDebugLevelTransformer() );

//			addRule( new BlockingHashJoinAnnotator() );
//			addRule( new HashJoinBlockingHashJoinAnnotator() );

			addRule( new WholeGraphStepPartitioner() );

			addRule( new WholeGraphNodePartitioner() );
		}
	}

}
