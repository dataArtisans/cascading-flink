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
 * Injects a Boundary before a sink tap in order to split of the sink tap as a separate node.
 */
public class BoundaryBeforeSinkTapTransformer extends RuleInsertionTransformer
{
	public BoundaryBeforeSinkTapTransformer() {
		super(
				BalanceAssembly,
				new SinkTapMatcher(),
				BoundaryElementFactory.BOUNDARY_FACTORY,
				InsertionGraphTransformer.Insertion.Before
		);
	}

	public static class SinkTapMatcher extends RuleExpression
	{
		public SinkTapMatcher()
		{
			super( new SinkTapGraph() );
		}
	}

	public static class SinkTapGraph extends ExpressionGraph {

		public SinkTapGraph() {

			super(SearchOrder.ReverseTopological);

			arc(
					not(
							OrElementExpression.or(
									new FlowElementExpression(Extent.class),
									new FlowElementExpression(Boundary.class)
							)
						),
					ScopeExpression.ANY,
					new FlowElementExpression(ElementCapture.Primary, Tap.class)

				);
		}
	}


}
