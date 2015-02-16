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

import cascading.flow.BaseFlow;
import cascading.flow.FlowProcess;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Map;

public class FlinkFlow extends BaseFlow<FlinkConfig> {

	private ExecutionEnvironment flinkEnv;

	public FlinkFlow(ExecutionEnvironment env) {
		this.flinkEnv = env;
	}

	@Override
	protected void initConfig(Map map, FlinkConfig o) {
		// nothing to do
	}


	@Override
	protected void setConfigProperty(FlinkConfig config, Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected FlinkConfig newConfig(FlinkConfig o) {

		return new FlinkConfig();
	}

	@Override
	protected void internalStart() {
		// not sure what to do here...

		try {
			flinkEnv.execute();
		} catch(Exception e) {
			throw new RuntimeException("Flink Execution failed...", e);
		}

	}

	@Override
	protected void internalClean(boolean b) {
		// not sure what to do here...
		throw new UnsupportedOperationException();
	}

	@Override
	protected int getMaxNumParallelSteps() {
		return flinkEnv.getDegreeOfParallelism();
	}

	@Override
	protected void internalShutdown() {
		// not sure what to do here...
		throw new UnsupportedOperationException();
	}

	@Override
	public FlinkConfig getConfig() {
		return new FlinkConfig();
	}

	@Override
	public FlinkConfig getConfigCopy() {
		return new FlinkConfig();
	}

	@Override
	public Map<Object, Object> getConfigAsProperties() {
		return null;
	}

	@Override
	public String getProperty(String s) {
		// not sure what to do here...
		throw new UnsupportedOperationException();

	}

	@Override
	public FlowProcess getFlowProcess() {
		// not sure what to do here...
//		throw new UnsupportedOperationException();
		return new FlinkFlowProcess(this.getFlowSession());
	}

	@Override
	public boolean stepsAreLocal() {
		return false;
	}
}
