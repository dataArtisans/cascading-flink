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
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistrySet;
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
		return new RuleRegistrySet();
	}

}
