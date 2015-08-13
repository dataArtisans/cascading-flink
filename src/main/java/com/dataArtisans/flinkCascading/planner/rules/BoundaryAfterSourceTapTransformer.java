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

import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.OrElementExpression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.iso.transformer.InsertionGraphTransformer;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;
import cascading.pipe.Boundary;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.NotElementExpression.not;
import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 * Injects a Boundary after a source Tap in order to split of the source Tap as a separate node.
 */
public class BoundaryAfterSourceTapTransformer extends RuleInsertionTransformer
{
	public BoundaryAfterSourceTapTransformer() {
		super(
				BalanceAssembly,
				new SourceTapMatcher(),
				BoundaryElementFactory.BOUNDARY_FACTORY,
				InsertionGraphTransformer.Insertion.After
		);
	}

	public static class SourceTapMatcher extends RuleExpression
	{
		public SourceTapMatcher()
		{
			super( new SourceTapGraph() );
		}
	}

	public static class SourceTapGraph extends ExpressionGraph {

		public SourceTapGraph() {

			super(SearchOrder.ReverseTopological);

			arc(
					new FlowElementExpression(ElementCapture.Primary, Tap.class),
					ScopeExpression.ANY,
					not(
							OrElementExpression.or(
									new FlowElementExpression(Extent.class),
									new FlowElementExpression(Boundary.class)
							)
						)
			);
		}
	}


}
