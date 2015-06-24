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

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.transformer.RuleReplaceTransformer;
import cascading.pipe.Boundary;
import cascading.pipe.GroupBy;

/**
 * Removes Boundaries in front of GroupBys
 */
public class BoundaryBeforeGroupByRemovalTransformer extends RuleReplaceTransformer {

	public BoundaryBeforeGroupByRemovalTransformer() {
		super(PlanPhase.PostResolveAssembly, new BoundaryGroupByMatcher());
	}

	public static class BoundaryGroupByMatcher extends RuleExpression {

		public BoundaryGroupByMatcher() {
			super(
					(new ExpressionGraph()).
							arc(
									new FlowElementExpression(ElementCapture.Primary, Boundary.class),
									ScopeExpression.ALL,
									new FlowElementExpression(ElementCapture.Secondary, GroupBy.class)));
		}
	}

}
