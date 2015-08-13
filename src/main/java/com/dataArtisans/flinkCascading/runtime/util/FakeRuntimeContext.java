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

package com.dataArtisans.flinkCascading.runtime.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Flink DataSources and DataSinks do not provide a RuntimeContext yet.
 * We use this FakeRuntimeContext instead until this feature is available.
 *
 */
public class FakeRuntimeContext implements RuntimeContext {


	private String name;
	private int taskNum;

	public void setName(String name) {
		this.name = name;
	}

	public void setTaskNum(int taskNum) {
		this.taskNum = taskNum;
	}

	@Override
	public String getTaskName() {
		return name;
	}

	@Override
	public int getNumberOfParallelSubtasks() {
		return 0;
	}

	@Override
	public int getIndexOfThisSubtask() {
		return taskNum;
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return null;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return null;
	}

	@Override
	public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

	}

	@Override
	public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
		return null;
	}

	@Override
	public HashMap<String, Accumulator<?, ?>> getAllAccumulators() {
		return null;
	}

	@Override
	public IntCounter getIntCounter(String name) {
		return null;
	}

	@Override
	public LongCounter getLongCounter(String name) {
		return null;
	}

	@Override
	public DoubleCounter getDoubleCounter(String name) {
		return null;
	}

	@Override
	public Histogram getHistogram(String name) {
		return null;
	}

	@Override
	public <RT> List<RT> getBroadcastVariable(String name) {
		return null;
	}

	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		return null;
	}

	@Override
	public DistributedCache getDistributedCache() {
		return null;
	}

	@Override
	public <S, C extends Serializable> OperatorState<S> getOperatorState(String s, S s1, boolean b, StateCheckpointer<S, C> stateCheckpointer) throws IOException {
		return null;
	}

	@Override
	public <S extends Serializable> OperatorState<S> getOperatorState(String s, S s1, boolean b) throws IOException {
		return null;
	}
}
