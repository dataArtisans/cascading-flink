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

package com.dataArtisans.flinkCascading.planning.rules;

import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.OrElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.elementexpression.BoundariesElementExpression;
import cascading.flow.planner.rule.partitioner.ExpressionRulePartitioner;
import cascading.flow.stream.graph.IORole;
import cascading.pipe.Boundary;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.NotElementExpression.not;
import static cascading.flow.planner.iso.expression.OrElementExpression.or;
import static cascading.flow.planner.rule.PlanPhase.PartitionNodes;

public class BottomUpBoundariesNodePartitioner extends ExpressionRulePartitioner
{

	public BottomUpBoundariesNodePartitioner() {
		super(
				PartitionNodes,

				new RuleExpression(
						new NoGroupJoinMergeBoundaryTapExpressionGraph(),
						new BottomUpNoSplitConsecutiveBoundariesExpressionGraph()
				),

				new ElementAnnotation( ElementCapture.Primary, IORole.sink )
		);
	}

	public static class NoGroupJoinMergeBoundaryTapExpressionGraph extends ExpressionGraph {
		public NoGroupJoinMergeBoundaryTapExpressionGraph() {
			super(
					not(
							OrElementExpression.or(
									ElementCapture.Primary,
									new FlowElementExpression(Extent.class),
									new FlowElementExpression(Group.class),
									new FlowElementExpression(HashJoin.class),
									new FlowElementExpression(Merge.class),
									new FlowElementExpression(Boundary.class),
									new FlowElementExpression(Tap.class)
							)
					)
			);
		}
	}

	public static class BottomUpNoSplitConsecutiveBoundariesExpressionGraph extends ExpressionGraph
	{
		public BottomUpNoSplitConsecutiveBoundariesExpressionGraph()
		{
			super( SearchOrder.ReverseTopological );

			this.arc(
					or(
							new FlowElementExpression( Boundary.class, TypeExpression.Topo.LinearOut ),
							new FlowElementExpression( Tap.class, TypeExpression.Topo.LinearOut ),
							new FlowElementExpression( Group.class, TypeExpression.Topo.LinearOut ),
							new FlowElementExpression( Merge.class, TypeExpression.Topo.LinearOut )
					),

					PathScopeExpression.ANY,

					new BoundariesElementExpression( ElementCapture.Primary )
			);
		}
	}


}
