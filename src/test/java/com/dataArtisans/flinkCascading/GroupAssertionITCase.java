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
import cascading.operation.AssertionLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertGroupSizeMoreThan;
import cascading.operation.buffer.FirstNBuffer;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class GroupAssertionITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(1);
		inPipeDataMap.put("tokens", TestData.getTextData());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {
		return Collections.singleton("first3");
	}

	@Override
	public FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );
		Fields count = new Fields( "count" );

		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		Pipe tokensPipe = new Each( "tokens", text, splitter, Fields.RESULTS );

		Pipe cntPipe = new Pipe( "counts", tokensPipe );
		cntPipe = new GroupBy( cntPipe, token );
		cntPipe = new Every( cntPipe, Fields.ALL, new Count(), Fields.ALL );

		Pipe first3Pipe = new GroupBy( "first3", cntPipe, count, token);
//		first3Pipe = new Every( first3Pipe, Fields.ALL, AssertionLevel.STRICT, new AssertGroupSizeMoreThan(0));
		first3Pipe = new Every( first3Pipe, count, new FirstNBuffer(3), Fields.REPLACE);

		FlowDef flowDef = FlowDef.flowDef().setName( "first3" )
				.addTail(first3Pipe);

		return flowDef;
	}
}
