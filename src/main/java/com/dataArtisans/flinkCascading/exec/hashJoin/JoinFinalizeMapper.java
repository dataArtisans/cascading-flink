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

package com.dataArtisans.flinkCascading.exec.hashJoin;

import cascading.flow.FlowNode;
import cascading.pipe.HashJoin;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.util.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class JoinFinalizeMapper extends RichMapPartitionFunction<Tuple2<Tuple, Tuple[]>, Tuple> {

	private static final Logger LOG = LoggerFactory.getLogger(JoinFinalizeMapper.class);

	private FlowNode flowNode;
	private HashJoin join;
	private Fields[] keyFields;
	private Fields[] valueFields;

	private FlinkFlowProcess currentProcess;

	public JoinFinalizeMapper() {}

	public JoinFinalizeMapper(FlowNode flowNode, HashJoin join, Fields[] keyFields, Fields[] valueFields) {

		this.flowNode = flowNode;
		this.join = join;
		this.keyFields = keyFields;
		this.valueFields = valueFields;
	}

	@Override
	public void open(Configuration config) {

		String taskId = "hashJoin-" + flowNode.getID();
		this.currentProcess = new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config), this.getRuntimeContext(), taskId);

	}

	@Override
	public void mapPartition(Iterable<Tuple2<Tuple, Tuple[]>> input, Collector<Tuple> output) throws Exception {

		Joiner joiner = join.getJoiner();

		FlinkJoinClosure closure = new FlinkJoinClosure(this.currentProcess, keyFields, valueFields);

		for(Tuple2<Tuple, Tuple[]> t : input) {
			closure.reset(t);

			Iterator<Tuple> joinedTuples = joiner.getIterator(closure);
			while(joinedTuples.hasNext()) {
				output.collect(joinedTuples.next());
			}
		}
	}

}
