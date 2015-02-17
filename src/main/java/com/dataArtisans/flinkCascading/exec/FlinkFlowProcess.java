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

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FlinkFlowProcess extends FlowProcess<Configuration> {

	private transient RuntimeContext runtimeContext;

	Configuration conf;

	int numTasks;
	int taskId;

	public FlinkFlowProcess() {}

	public FlinkFlowProcess(Configuration conf) {
		this.conf = conf;
	}

	public FlinkFlowProcess(Configuration conf, RuntimeContext runtimeContext) {

		this(conf);
		this.runtimeContext = runtimeContext;

		this.numTasks = runtimeContext.getNumberOfParallelSubtasks();
		this.taskId = runtimeContext.getIndexOfThisSubtask();
	}

	public FlinkFlowProcess(RuntimeContext runtimeContext) {
		this(new Configuration(), runtimeContext);
	}

	public FlinkFlowProcess(Configuration conf, int numTasks, int taskId) {
		this(conf);
		this.numTasks = numTasks;
		this.taskId = taskId;
	}

	@Override
	public FlowProcess copyWith(Configuration entries) {
		return new FlinkFlowProcess(entries);
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
	public Object getProperty( String key ) {
		return this.conf.get(key);
	}

	@Override
	public Collection<String> getPropertyKeys() {
		Set<String> keys = new HashSet<String>();

		for( Map.Entry<String, String> entry : this.conf )
			keys.add( entry.getKey() );

		return Collections.unmodifiableSet(keys);
	}

	@Override
	public Object newInstance(String s) {
		// create instance with class loader
		return null;
	}

	@Override
	public void keepAlive() {
		// Tez doesn't do anything here either...
	}

	@Override
	public void increment(Enum anEnum, long l) {
		// incr. counter
	}

	@Override
	public void increment(String s, String s1, long l) {
		// incr. counter
	}

	@Override
	public void setStatus(String s) {
		// Tez doesn't do anything here either
	}

	@Override
	public boolean isCounterStatusInitialized() {
		return false; // should be fine
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
		return null; // not sure what this is for
	}

	@Override
	public TupleEntryCollector openSystemIntermediateForWrite() throws IOException {
		return null; // Tez does nothing either
	}

	@Override
	public Configuration getConfigCopy() {
		return new Configuration(conf); // TODO
	}

	@Override
	public <C> C copyConfig(C c) {
		return (C)new Configuration(conf); // TODO;
	}

	@Override
	public <C> Map<String, String> diffConfigIntoMap(C c, C c1) {
		return null;
	}

	@Override
	public Configuration mergeMapIntoConfig(Configuration entries, Map<String, String> map) {
		return null;
	}

}
