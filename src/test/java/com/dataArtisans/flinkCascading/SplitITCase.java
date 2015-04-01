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

package com.dataArtisans.flinkCascading;

import cascading.flow.FlowDef;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class SplitITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(1);
		inPipeDataMap.put("split", TestData.getTextData());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {

		Set<String> outPipes = new HashSet<String>();
		outPipes.add("left");
		outPipes.add("right");
		return outPipes;
	}

	@Override
	public FlowDef getFlow() {

		Pipe pipe = new Pipe("split");
		Each pipe1 = new Each(pipe, new Fields(new Comparable[]{"line"}), new RegexFilter(".*"));
		Each left = new Each(new Pipe("left", pipe1), new Fields(new Comparable[]{"line"}), new RegexFilter(".*"));
		Each right = new Each(new Pipe("right", pipe1), new Fields(new Comparable[]{"line"}), new RegexFilter(".*"));

		FlowDef flowDef = FlowDef.flowDef().addTails(left, right);

		return flowDef;
	}


}
