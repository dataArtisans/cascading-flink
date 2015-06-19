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
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerInfo;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.tap.Tap;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FlinkPlanner extends FlowPlanner<FlinkFlow, Configuration> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPlanner.class);

	private Configuration defaultConfig;

	private ExecutionEnvironment env;

	@Override
	public Configuration getDefaultConfig() {
	return defaultConfig;
}

	@Override
	public PlannerInfo getPlannerInfo(String s) {
		return new PlannerInfo("Flink Planner", "Apache Flink", "???"); // TODO
	}

	@Override
	public PlatformInfo getPlatformInfo() {
		return new PlatformInfo("Apache Flink", "data Artisans GmbH", "0.1");
	}

	@Override
	public void initialize( FlowConnector flowConnector, Map<Object, Object> properties ) {

		super.initialize( flowConnector, properties );

		this.env = ExecutionEnvironment.getExecutionEnvironment(); // TODO: replace by configured ExecutionEnvironment

		defaultConfig = new Configuration(); // TODO: copy properties

		// TODO: set JAR file
//		Class type = AppProps.getApplicationJarClass(properties);
//		if( defaultConfig.getJar() == null && type != null ) {
//			defaultJobConf.setJarByClass(type);
//		}
//		String path = AppProps.getApplicationJarPath(properties);
//		if( defaultJobConf.getJar() == null && path != null ) {
//			defaultJobConf.setJar(path);
//		}
//		if( defaultJobConf.getJar() == null ) {
//			defaultJobConf.setJarByClass(HadoopUtil.findMainClass(HadoopPlanner.class));
//		}
//		AppProps.setApplicationJarPath(properties, defaultJobConf.getJar());
//		LOG.info( "using application jar: {}", defaultJobConf.getJar() );
}

	@Override
	public void configRuleRegistryDefaults( RuleRegistry ruleRegistry ) {
		super.configRuleRegistryDefaults( ruleRegistry );
	}

	public FlowStep<Configuration> createFlowStep( ElementGraph stepElementGraph, FlowNodeGraph flowNodeGraph ) {
		return new FlinkFlowStep( env, stepElementGraph, flowNodeGraph );
	}

	@Override
	protected FlinkFlow createFlow( FlowDef flowDef ) {
		return new FlinkFlow(env, getPlatformInfo(), flowDef, getDefaultProperties(), getDefaultConfig());
	}

	@Override
	protected Tap makeTempTap(String prefix, String name) {
		return null;  // not required for Flink
	}

}
