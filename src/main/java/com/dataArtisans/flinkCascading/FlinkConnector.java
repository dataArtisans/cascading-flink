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

package com.dataArtisans.flinkCascading;

import cascading.flow.FlowConnector;
import cascading.flow.FlowElement;
import cascading.flow.hadoop.planner.rule.assertion.DualStreamedAccumulatedMergePipelineAssert;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsStepPartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.GroupTapNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.MultiTapGroupNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedAccumulatedTapsPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedOnlySourcesPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedSelfJoinSourcesPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.TapGroupTapStepPartitioner;
import cascading.flow.hadoop.planner.rule.transformer.RemoveMalformedHashJoinPipelineTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceCheckpointTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupNonBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupSplitMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceHashJoinBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceHashJoinSameSourceTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceNonSafePipeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceNonSafeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceSameSourceStreamedAccumulatedTransformer;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.graph.ElementGraph;
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
import cascading.flow.planner.rule.transformer.BoundaryElementFactory;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;
import cascading.pipe.Boundary;
import cascading.scheme.Scheme;
import com.dataArtisans.flinkCascading.planning.FlinkPlanner;
import com.dataArtisans.flinkCascading.planning.rules.BottomUpBoundariesNodePartitioner;
import com.dataArtisans.flinkCascading.planning.rules.SinkTapBoundaryTransformer;
import com.dataArtisans.flinkCascading.planning.rules.SourceTapBoundaryTransformer;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class FlinkConnector extends FlowConnector {


	public FlinkConnector(ExecutionEnvironment env) {
		this(env, new HashMap<Object, Object>());
	}

	public FlinkConnector(ExecutionEnvironment env, Map<Object, Object> properties) {
		super(properties);
	}

	@Override
	protected Class<? extends Scheme> getDefaultIntermediateSchemeClass() {
		return null; // not required for Flink
	}

	@Override
	protected FlowPlanner createFlowPlanner() {
		return new FlinkPlanner();
	}

	@Override
	protected RuleRegistrySet createDefaultRuleRegistrySet() {

		return new RuleRegistrySet(new FlinkDagRuleRegistry());
	}

	public static class FlinkMRRuleRegistry extends RuleRegistry {

		public FlinkMRRuleRegistry() {
			enableDebugLogging();

			// PreBalance
			addRule( new LoneGroupAssert() );
			addRule( new MissingGroupAssert() );
			addRule( new BufferAfterEveryAssert() );
			addRule( new EveryAfterBufferAssert() );
			addRule( new SplitBeforeEveryAssert() );

			// Balance with temporary Taps
			addRule( new TapBalanceGroupSplitTransformer() );
			addRule( new TapBalanceGroupSplitMergeGroupTransformer() );
			addRule( new TapBalanceGroupMergeGroupTransformer() );
			addRule( new TapBalanceGroupGroupTransformer() );
			addRule( new TapBalanceCheckpointTransformer() );
			addRule( new TapBalanceHashJoinSameSourceTransformer() );
			addRule( new TapBalanceHashJoinBlockingHashJoinTransformer() );
			addRule( new TapBalanceGroupBlockingHashJoinTransformer() );
			addRule( new TapBalanceGroupNonBlockingHashJoinTransformer() );
			addRule( new TapBalanceSameSourceStreamedAccumulatedTransformer() );
			addRule( new TapBalanceNonSafeSplitTransformer() );
			addRule( new TapBalanceNonSafePipeSplitTransformer() );

			// PreResolve
			addRule( new RemoveNoOpPipeTransformer() );
			addRule( new ApplyAssertionLevelTransformer() );
			addRule( new ApplyDebugLevelTransformer() );

			// PostResolve
			// addRule( new CombineAdjacentTapTransformer() );

			// PartitionSteps
			addRule( new ConsecutiveTapsStepPartitioner() );
			addRule( new TapGroupTapStepPartitioner() );

			// PartitionNodes
			addRule( new ConsecutiveTapsNodePartitioner() );
			addRule( new MultiTapGroupNodePartitioner() );
			addRule( new GroupTapNodePartitioner() );

			// PartitionPipelines
			addRule( new StreamedAccumulatedTapsPipelinePartitioner() );
			addRule( new StreamedSelfJoinSourcesPipelinePartitioner() );
			addRule( new StreamedOnlySourcesPipelinePartitioner() );

			// PostPipelines
			addRule( new RemoveMalformedHashJoinPipelineTransformer() );

			// remove when GraphFinder supports captured edges
			addRule( new DualStreamedAccumulatedMergePipelineAssert() );

			// enable when GraphFinder supports captured edges
			// addRule( new RemoveStreamedBranchTransformer() );
		}
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

//			addRule( new BoundaryBalanceGroupSplitSpliceTransformer() ); // prevents AssemblyHelpersPlatformTest#testSameSourceMerge deadlock
//			addRule( new BoundaryBalanceCheckpointTransformer() );

			// Balance
			// inject boundaries after source taps and before sink taps
			addRule( new SourceTapBoundaryTransformer() );
			addRule( new SinkTapBoundaryTransformer() );

			// hash join
//			addRule( new BoundaryBalanceHashJoinSameSourceTransformer() );
//			addRule( new BoundaryBalanceHashJoinToHashJoinTransformer() ); // force HJ into unique nodes
//			addRule( new BoundaryBalanceGroupBlockingHashJoinTransformer() ); // joinAfterEvery

//			addRule( new BoundaryBalanceGroupSplitHashJoinTransformer() ); // groupBySplitJoins

			// PreResolve
			addRule( new RemoveNoOpPipeTransformer() );
			addRule( new ApplyAssertionLevelTransformer() );
			addRule( new ApplyDebugLevelTransformer() );

			// PostResolve

			// PartitionSteps
			addRule( new WholeGraphStepPartitioner() );

			// PostSteps

			// PartitionNodes

			// no match with HashJoin inclusion
//			addRule( new TopDownSplitBoundariesNodePartitioner() ); // split from source to multiple sinks
//			addRule( new ConsecutiveGroupOrMergesNodePartitioner() );
			addRule( new BottomUpBoundariesNodePartitioner() ); // streamed paths re-partitioned w/ StreamedOnly
//			addRule( new SplitJoinBoundariesNodeRePartitioner() ); // testCoGroupSelf - compensates for tez-1190

			// hash join inclusion
//			addRule( new BottomUpJoinedBoundariesNodePartitioner() ); // will capture multiple inputs into sink for use with HashJoins
//			addRule( new StreamedAccumulatedBoundariesNodeRePartitioner() ); // joinsIntoCoGroupLhs & groupBySplitJoins
//			addRule( new StreamedOnlySourcesNodeRePartitioner() );

			// PostNodes
//			addRule( new RemoveMalformedHashJoinNodeTransformer() ); // joinsIntoCoGroupLhs
//			addRule( new AccumulatedPostNodeAnnotator() ); // allows accumulated boundaries to be identified

//			addRule( new DualStreamedAccumulatedMergeNodeAssert() );

			this.addElementFactory(BoundaryElementFactory.BOUNDARY_PIPE, new IntermediateBoundaryElementFactory());
		}

		public static class IntermediateBoundaryElementFactory extends BoundaryElementFactory {

			@Override
			public FlowElement create( ElementGraph graph, FlowElement flowElement )
			{
				return new Boundary();
			}
		}

	}

}
