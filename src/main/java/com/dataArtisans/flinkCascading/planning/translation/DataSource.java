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

import cascading.flow.planner.graph.FlowElementGraph;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.FileTapInputFormat;
import com.dataArtisans.flinkCascading.exec.operators.HfsInputFormat;
import com.dataArtisans.flinkCascading.types.CascadingTupleTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


public class DataSource extends Operator {

	private Tap tap;

	public DataSource(Tap tap, FlowElementGraph flowGraph) {
		super(Collections.EMPTY_LIST, tap, tap, flowGraph);
		this.tap = tap;

	}

	@Override
	protected DataSet translateToFlink(ExecutionEnvironment env,
										List<DataSet> inputs, List<Operator> inputOps,
										Configuration config) {

		if(tap instanceof Hfs) {
			return translateHfsSource((Hfs) tap, env);
		}
		else if(tap instanceof FileTap) {
			return translateFileTapSource((FileTap) tap, env);
		}
		else if(tap instanceof MultiSourceTap) {
			return translateMultiSourceTap((MultiSourceTap) tap, env);
		} else {
			throw new RuntimeException("Tap type "+tap.getClass().getCanonicalName()+" not supported yet.");
		}

	}

	private DataSet translateHfsSource(Hfs tap, ExecutionEnvironment env) {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		tap.getScheme().sourceConfInit(null, tap, conf);
		conf.set("mapreduce.input.fileinputformat.inputdir", tap.getPath().toString());

		DataSet<Tuple> src = env
				.createInput(new HfsInputFormat(tap, conf), new CascadingTupleTypeInfo())
				.name(tap.getIdentifier());

		return src;
	}

	private DataSet translateFileTapSource(FileTap tap, ExecutionEnvironment env) {

		Properties conf = new Properties();
		tap.getScheme().sourceConfInit(null, tap, conf);

		DataSet<Tuple> src = env
				.createInput(new FileTapInputFormat(tap, conf), new CascadingTupleTypeInfo())
				.name(tap.getIdentifier())
				.setParallelism(1);

		return src;
	}

	private DataSet translateMultiSourceTap(MultiSourceTap tap, ExecutionEnvironment env) {
		Iterator<Tap> childTaps = ((MultiSourceTap)tap).getChildTaps();

		DataSet cur = null;
		while(childTaps.hasNext()) {
			Tap childTap = childTaps.next();
			DataSet source;
			if(childTap instanceof Hfs) {
				source = translateHfsSource((Hfs)childTap, env);
			}
			else if(childTap instanceof FileTap) {
				source = translateFileTapSource((FileTap)childTap, env);
			}
			else {
				throw new RuntimeException("Tap type "+tap.getClass().getCanonicalName()+" not supported yet.");
			}

			if(cur == null) {
				cur = source;
			}
			else {
				cur = cur.union(source);
			}
		}

		return cur;
	}

}
