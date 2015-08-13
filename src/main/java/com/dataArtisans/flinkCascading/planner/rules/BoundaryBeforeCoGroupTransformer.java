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

package com.dataArtisans.flinkCascading.planner.rules;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.iso.transformer.InsertionGraphTransformer;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;
import cascading.pipe.CoGroup;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 * Injects a Boundary before a CoGroup in order to split of the CoGroup as a separate node
 * and have all predecessors in individual pipes.
 */
public class BoundaryBeforeCoGroupTransformer extends RuleInsertionTransformer
{
	public BoundaryBeforeCoGroupTransformer() {
		super(
				BalanceAssembly,
				new CoGroupMatcher(),
				BoundaryElementFactory.BOUNDARY_FACTORY,
				InsertionGraphTransformer.Insertion.BeforeEachEdge
		);
	}

	public static class CoGroupMatcher extends RuleExpression
	{
		public CoGroupMatcher()
		{
			super( new CoGroupGraph() );
		}
	}

	public static class CoGroupGraph extends ExpressionGraph {

		public CoGroupGraph() {

			super(SearchOrder.ReverseTopological, new FlowElementExpression(ElementCapture.Primary, CoGroup.class));

		}
	}

}
