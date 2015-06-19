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

package com.dataArtisans.flinkCascading.exec;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FlinkFlowProcess extends FlowProcess<Configuration> {


	private transient RuntimeContext runtimeContext;
	private Configuration conf;
	private int numTasks = -1;
	private int taskId = -1;

	public FlinkFlowProcess() {}

	public FlinkFlowProcess(Configuration conf) {
		this.conf = conf;
	}

	public FlinkFlowProcess(FlowSession flowSession, Configuration conf) {
		super(flowSession);
		this.conf = conf;
	}

	public FlinkFlowProcess(Configuration conf, RuntimeContext runtimeContext) {

		this(conf);
		this.runtimeContext = runtimeContext;
		this.numTasks = runtimeContext.getNumberOfParallelSubtasks();
		this.taskId = runtimeContext.getIndexOfThisSubtask();
	}

	public FlinkFlowProcess(Configuration conf, int numTasks, int taskId) {
		this(conf);
		this.numTasks = numTasks;
		this.taskId = taskId;
	}

	public FlinkFlowProcess(Configuration conf, FlinkFlowProcess process) {
		this(conf);
		this.runtimeContext = process.runtimeContext;
		this.numTasks = process.numTasks;
		this.taskId = process.taskId;
	}

	@Override
	public int getNumProcessSlices() {
		return this.numTasks;
	}

	@Override
	public int getCurrentSliceNum() {
		return this.taskId;
	}

	@Override
	public FlowProcess copyWith(Configuration config) {
		return new FlinkFlowProcess(config, this);
	}

	@Override
	public Object getProperty( String key ) {
		return this.conf.getString(key, null);
	}

	@Override
	public Collection<String> getPropertyKeys() {
		Set<String> keys = conf.keySet();

		return Collections.unmodifiableSet( keys );
	}

	@Override
	public Object newInstance(String className) {
		if(className == null || className.isEmpty()) {
			return null;
		}

		try {
			Class clazz = Class.forName(className);
			return InstantiationUtil.instantiate(clazz);
		}
		catch( ClassNotFoundException exception ) {
			throw new CascadingException( "unable to load class: " + className.toString(), exception );
		}
	}

	@Override
	public void keepAlive() {
		// Hadoop reports progress
		// Tez doesn't do anything here either...
	}

	@Override
	public void increment(Enum e, long l) {
//		throw new UnsupportedOperationException("Enum counters not supported."); // TODO
	}

	@Override
	public void increment(String group, String counter, long l) {
		if(this.runtimeContext == null) {
			throw new RuntimeException("RuntimeContext has not been set.");
		}
		else {
			this.runtimeContext.getLongCounter(group+"."+counter).add(l);
		}
	}

	@Override
	public long getCounterValue(Enum anEnum) {
//		throw new UnsupportedOperationException("Enum counters not supported."); // TODO
		return -1l;
	}

	@Override
	public long getCounterValue(String group, String counter) {
		if(this.runtimeContext == null) {
			throw new RuntimeException("RuntimeContext has not been set.");
		}
		else {
			return this.runtimeContext.getLongCounter(group+"."+counter).getLocalValue();
		}
	}

	@Override
	public void setStatus(String s) {
		// Tez doesn't do anything here either
	}

	@Override
	public boolean isCounterStatusInitialized() {
		return this.runtimeContext != null;
	}

	@Override
	public TupleEntryIterator openTapForRead(Tap tap) throws IOException {
		return tap.openForRead( this );
	}

	@Override
	public TupleEntryCollector openTapForWrite(Tap tap) throws IOException {
		return tap.openForWrite( this, null ); // do not honor sinkmode as this may be opened across tasks
	}

	@Override
	public TupleEntryCollector openTrapForWrite(Tap tap) throws IOException {
		return null; // not sure if required for Flink
	}

	@Override
	public TupleEntryCollector openSystemIntermediateForWrite() throws IOException {
		return null; // Not required for Flink
	}

	@Override
	public Configuration getConfig() {
		return conf;
	}

	@Override
	public Configuration getConfigCopy() {
		return this.conf.clone();
	}

	@Override
	public <C> C copyConfig(C conf) {
		return (C)((Configuration)conf).clone();
	}

	@Override
	public <C> Map<String, String> diffConfigIntoMap(C defaultConfig, C updatedConfig) {

		Map<String, String> newConf = new HashMap<String, String>();
		for(String key : ((Configuration)updatedConfig).keySet()) {
			String val = ((Configuration) updatedConfig).getString(key, null);
			String defaultVal = ((Configuration)defaultConfig).getString(key, null);

			// add keys that are different from default
			if((val == null && defaultVal == null) || val.equals(defaultVal)) {
				continue;
			}
			else {
				newConf.put(key, val);
			}
		}

		return newConf;
	}

	@Override
	public Configuration mergeMapIntoConfig(Configuration defaultConfig, Map<String, String> map) {

		Configuration mergedConf = defaultConfig.clone();
		for(String key : map.keySet()) {
			mergedConf.setString(key, map.get(key));
		}
		return mergedConf;
	}

}
