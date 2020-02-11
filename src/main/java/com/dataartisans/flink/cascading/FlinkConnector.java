/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.flow.planner.rule.assertion.BufferAfterEveryAssert;
import cascading.flow.planner.rule.assertion.EveryAfterBufferAssert;
import cascading.flow.planner.rule.assertion.LoneGroupAssert;
import cascading.flow.planner.rule.assertion.MissingGroupAssert;
import cascading.flow.planner.rule.assertion.SplitBeforeEveryAssert;
import cascading.flow.planner.rule.partitioner.WholeGraphStepPartitioner;
import cascading.flow.planner.rule.transformer.ApplyAssertionLevelTransformer;
import cascading.flow.planner.rule.transformer.ApplyDebugLevelTransformer;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;
import cascading.scheme.Scheme;
import com.dataartisans.flink.cascading.planner.rules.BoundaryAfterSplitEdgeTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryBeforeCoGroupTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryBeforeSinkTapTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryElementFactory;
import com.dataartisans.flink.cascading.planner.FlinkPlanner;
import com.dataartisans.flink.cascading.planner.rules.BottomUpBoundariesNodePartitioner;
import com.dataartisans.flink.cascading.planner.rules.BoundaryAfterMergeTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryAfterSplitNodeTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryBeforeGroupByTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryBeforeHashJoinTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryBeforeMergeTransformer;
import com.dataartisans.flink.cascading.planner.rules.BoundaryAfterSourceTapTransformer;
import com.dataartisans.flink.cascading.planner.rules.DoubleBoundaryRemovalTransformer;
import com.dataartisans.flink.cascading.planner.rules.TopDownSplitBoundariesNodePartitioner;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkConnector extends FlowConnector {

	List<String> classPath = new ArrayList<String>();

	private ExecutionEnvironment env;

	public FlinkConnector() {
		this(new Properties());
	}

	public FlinkConnector(Map<Object, Object> properties) {
		this(ExecutionEnvironment.getExecutionEnvironment(), properties);
	}

	public FlinkConnector(ExecutionEnvironment env, Map<Object, Object> properties) {

		super(properties);
		this.env = env;
	}

	@Override
	protected Class<? extends Scheme> getDefaultIntermediateSchemeClass() {
		return null; // not required for Flink
	}

	@Override
	protected FlowPlanner createFlowPlanner() {
		return new FlinkPlanner(env, classPath);
	}

	@Override
	protected RuleRegistrySet createDefaultRuleRegistrySet() {

		return new RuleRegistrySet(new FlinkDagRuleRegistry());
	}

	@Override
	public Flow connect(FlowDef flowDef) {
		classPath.addAll(flowDef.getClassPath());
		return super.connect(flowDef);
	}

	public static class FlinkDagRuleRegistry extends RuleRegistry {

		public FlinkDagRuleRegistry() {

			enableDebugLogging();

			// PreBalance
			addRule( new LoneGroupAssert() );
			addRule( new MissingGroupAssert() );
			addRule( new BufferAfterEveryAssert() );
			addRule( new EveryAfterBufferAssert() );
			addRule( new SplitBeforeEveryAssert() );

			// Balance

			// inject boundaries after source taps
			addRule( new BoundaryAfterSourceTapTransformer() );
			// inject boundaries before sink taps
			addRule( new BoundaryBeforeSinkTapTransformer() );
			// inject boundaries before and after merges
			addRule( new BoundaryBeforeMergeTransformer() );
			addRule( new BoundaryAfterMergeTransformer() );
			// inject boundaries after each split node
			addRule( new BoundaryAfterSplitNodeTransformer() );
			addRule( new BoundaryAfterSplitEdgeTransformer() );
			// inject boundaries before co groups
			addRule( new BoundaryBeforeCoGroupTransformer() );
			// inject boundaries before group bys
			addRule( new BoundaryBeforeGroupByTransformer() );
			// inject boundaries before and after hash joins
			addRule( new BoundaryBeforeHashJoinTransformer() );

			// remove duplicate boundaries
			addRule( new DoubleBoundaryRemovalTransformer() );

			// PreResolve
			addRule( new RemoveNoOpPipeTransformer() );
			addRule( new ApplyAssertionLevelTransformer() );
			addRule( new ApplyDebugLevelTransformer() );

			// PostResolve

			// PartitionSteps
			addRule( new WholeGraphStepPartitioner() );

			// PostSteps

			// PartitionNodes

			addRule( new TopDownSplitBoundariesNodePartitioner() ); // split from source to multiple sinks
			addRule( new BottomUpBoundariesNodePartitioner() ); // streamed paths re-partitioned w/ StreamedOnly

			// Element Factories
			this.addElementFactory(BoundaryElementFactory.BOUNDARY_FACTORY, new BoundaryElementFactory());
		}

	}

}
