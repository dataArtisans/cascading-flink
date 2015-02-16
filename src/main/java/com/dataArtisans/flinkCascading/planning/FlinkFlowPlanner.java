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
import cascading.flow.FlowStep;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.tap.Tap;
import org.apache.flink.api.java.ExecutionEnvironment;


public class FlinkFlowPlanner extends FlowPlanner<FlinkFlow, FlinkConfig> {

	public ExecutionEnvironment env;

	public FlinkFlowPlanner(ExecutionEnvironment env) {
		this.env = env;
	}

	@Override
	public FlinkConfig getDefaultConfig() {
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
	public FlowStep<FlinkConfig> createFlowStep(ElementGraph elementGraph, FlowNodeGraph flowNodeGraph) {
		return null;
	}

	@Override
	protected Tap makeTempTap(String s, String s1) {
		return null;
	}

	@Override
	public FlinkFlow buildFlow( FlowDef flowDef, RuleRegistrySet ruleRegistrySet )
	{
		return null;
	}

}
