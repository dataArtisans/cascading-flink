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

package com.dataartisans.flink.cascading.planner;

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
import org.apache.flink.client.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class FlinkPlanner extends FlowPlanner<FlinkFlow, Configuration> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPlanner.class);

	private Configuration defaultConfig;

	private List<String> classPath;

	private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	public FlinkPlanner(List<String> classPath) {
		super();
		this.classPath = classPath;

		if (env.getParallelism() <= 0) {
			// load the default parallelism from config
			GlobalConfiguration.loadConfiguration(new File(CliFrontend.getConfigurationDirectoryFromEnv()).getAbsolutePath());
			org.apache.flink.configuration.Configuration configuration = GlobalConfiguration.getConfiguration();
			int parallelism = configuration.getInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY_OLD, -1);
			parallelism = configuration.getInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, parallelism);
			if (parallelism <= 0) {
				throw new RuntimeException("Please set the default parallelism via the -p command-line flag");
			} else {
				env.setParallelism(parallelism);
			}
		}

	}

	@Override
	public Configuration getDefaultConfig() {
	return defaultConfig;
}

	@Override
	public PlannerInfo getPlannerInfo(String registryName) {
		return new PlannerInfo(getClass().getSimpleName(), "Apache Flink", registryName);
	}

	@Override
	public PlatformInfo getPlatformInfo() {
		return new PlatformInfo("Apache Flink", "data Artisans GmbH", "0.1");
	}

	@Override
	public void initialize( FlowConnector flowConnector, Map<Object, Object> properties ) {

		super.initialize( flowConnector, properties );
		defaultConfig = createConfiguration(properties);
}

	@Override
	public void configRuleRegistryDefaults( RuleRegistry ruleRegistry ) {
		super.configRuleRegistryDefaults( ruleRegistry );
	}

	public FlowStep<Configuration> createFlowStep( ElementGraph stepElementGraph, FlowNodeGraph flowNodeGraph ) {
		return new FlinkFlowStep( env, stepElementGraph, flowNodeGraph, classPath );
	}

	@Override
	protected FlinkFlow createFlow( FlowDef flowDef ) {
		return new FlinkFlow(env, getPlatformInfo(), flowDef, getDefaultProperties(), getDefaultConfig());
	}

	@Override
	protected Tap makeTempTap(String prefix, String name) {
		return null;  // not required for Flink
	}

	public static Configuration createConfiguration( Map<Object, Object> properties ) {
		Configuration conf = new Configuration();
		copyProperties( conf, properties );
		return conf;
	}

	public static void copyProperties( Configuration config, Map<Object, Object> properties ) {
		if( properties instanceof Properties) {
			Properties props = (Properties) properties;
			Set<String> keys = props.stringPropertyNames();

			for( String key : keys ) {
				config.set(key, props.getProperty(key));
			}
		}
		else {
			for( Map.Entry<Object, Object> entry : properties.entrySet() ) {
				if( entry.getValue() != null ) {
					config.set(entry.getKey().toString(), entry.getValue().toString());
				}
			}
		}
	}

}
