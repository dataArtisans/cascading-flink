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

package com.dataArtisans.flinkCascading.planner;

import cascading.flow.BaseFlow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.PlatformInfo;
import com.dataArtisans.flinkCascading.runtime.util.FlinkFlowProcess;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import riffle.process.ProcessConfiguration;

import java.util.HashMap;
import java.util.Map;

public class FlinkFlow extends BaseFlow<Configuration> {

	private ExecutionEnvironment flinkEnv;
	private Configuration config;

	public FlinkFlow(ExecutionEnvironment env, PlatformInfo platformInfo, FlowDef flowDef, Map<Object, Object> properties, Configuration defaultConfig) {

		super(platformInfo, properties, defaultConfig, flowDef);
		this.flinkEnv = env;
	}

	@Override
	protected void initConfig(Map<Object, Object> properties, Configuration parentConfig) {
		if( properties != null ) {
			parentConfig = createConfig( properties, parentConfig );
		}

		if( parentConfig == null ) {
		// this is ok, getJobConf will pass a default parent in
			return;
		}

		config = HadoopUtil.copyJobConf(parentConfig);
	}


	@Override
	protected void setConfigProperty(Configuration config, Object key, Object value) {
		// don't let these objects pass, even though toString is called below.
		if( value instanceof Class || value instanceof JobConf || value instanceof Configuration) {
			return;
		}

		config.set(key.toString(), value.toString());
	}

	@Override
	protected Configuration newConfig(Configuration defaultConfig) {
		return defaultConfig == null ? new Configuration() : HadoopUtil.copyJobConf(defaultConfig);
	}

	@Override
	protected void internalStart() {
		// setup stuff: delete output files if overwrite, etc.
	}

	@Override
	protected void internalClean(boolean b) {
		// TODO: clean-up execution
	}

	@Override
	protected void internalShutdown() {
		// nothing to do?
	}

	@Override
	protected int getMaxNumParallelSteps() {
		return 1;
	}

	@Override
	public Configuration getConfig() {
		if( config == null ) {
			initConfig(null, new Configuration());
		}

		return config;
	}

	@ProcessConfiguration
	@Override
	public Configuration getConfigCopy() {
		return HadoopUtil.copyJobConf(getConfig());
	}

	@Override
	public Map<Object, Object> getConfigAsProperties() {
		Map<Object, Object> props = new HashMap<Object, Object>();

		Configuration conf = getConfig();
		for(Map.Entry<String, String> e : conf) {
			String key = e.getKey();
			props.put(key, conf.get(key));
		}

		return props;
	}

	@Override
	public String getProperty(String key) {
		return getConfig().get(key);

	}

	@Override
	public FlowProcess getFlowProcess() {
		return new FlinkFlowProcess(getFlowSession(), getConfig()); // TODO!!!
	}

	@Override
	public boolean stepsAreLocal() {
		return false;
	}


}
