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

package com.dataArtisans.flinkCascading.planning.translation;

import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.exec.operators.FileTapOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.HfsOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.ProjectionMapper;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Properties;


public class DataSink extends Operator {

	private Tap tap;

	public DataSink(Tap tap, Operator inputOp, FlowElementGraph flowGraph) {
		super(inputOp, tap, tap, flowGraph);
		this.tap = tap;
	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputs, List<Operator> inputOps) {

		if(inputs.size() != 1) {
			throw new RuntimeException("Each requires exactly one input");
		}

		DataSet tail = inputs.get(0);

		Fields tapFields = tap.getSinkFields();

		if(!tapFields.isAll()) {

			Scope scope = getIncomingScopeFrom(inputOps.get(0));
			Fields tailFields = scope.getIncomingTapFields();

			// check if we need to project
			if(!tapFields.equalsFields(tailFields)) {
				// add projection mapper
				tail = tail
						.map(new ProjectionMapper(tailFields, tapFields))
						.returns(new CascadingTupleTypeInfo())
						.name("Tap Projection Mapper");
			}
		}

		if (tap instanceof Hfs) {

			Hfs hfs = (Hfs) tap;
			Configuration conf = new Configuration();

			tail
					.output(new HfsOutputFormat(hfs, tapFields, conf))
					.setParallelism(1);
		}
		else if(tap instanceof FileTap) {

			FileTap fileTap = (FileTap) tap;
			Properties props = new Properties();

			tail
					.output(new FileTapOutputFormat(fileTap, tapFields, props))
					.setParallelism(1);
		}
		else {
			throw new RuntimeException("Unsupported Tap");
		}

		return null; // DataSink is not a DataSet
	}

}
