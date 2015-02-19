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

import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.dataArtisans.flinkCascading.exec.operators.HfsOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.util.List;


public class DataSink extends Operator {

	private static final boolean PRINT_STDOUT = true;

	private Tap tap;

	public DataSink(Tap tap) {
		this.tap = tap;
	}

	protected DataSet translateToFlink(ExecutionEnvironment env, List<DataSet> inputs) {

		if(inputs.size() != 1) {
			throw new RuntimeException("Each requires exactly one input");
		}

		if(PRINT_STDOUT) {
			inputs.get(0).print();
			return null;
		}

		if (tap instanceof Hfs) {

			Hfs hfs = (Hfs) tap;
			Configuration conf = new Configuration();

			inputs.get(0)
					.output(new HfsOutputFormat(hfs, conf))
					.setParallelism(1);

			return null;
		} else {
			throw new RuntimeException("Right now, only Hfs taps are supported.");
		}

	}

}
