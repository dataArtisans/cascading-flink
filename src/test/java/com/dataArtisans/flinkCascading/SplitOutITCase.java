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

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;
import data.InputData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class SplitOutITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(1);
		inPipeDataMap.put("lower1", TestData.getTextData());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {

		Set<String> outPipes = new HashSet<String>();
		outPipes.add("output1");
		outPipes.add("output2");
		return outPipes;
	}

	@Override
	public FlowDef getFlow() {

		Pipe pipeLower1 = new Pipe("lower1");
		GroupBy left = new GroupBy("output1", pipeLower1, new Fields(new Comparable[]{Integer.valueOf(0)}));
		GroupBy right = new GroupBy("output2", left, new Fields(new Comparable[]{Integer.valueOf(0)}));

		FlowDef flowDef = FlowDef.flowDef().addTails(left, right);

		return flowDef;
	}


}
