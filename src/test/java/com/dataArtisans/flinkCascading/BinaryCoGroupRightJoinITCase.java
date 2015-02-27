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
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class BinaryCoGroupRightJoinITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(2);
		inPipeDataMap.put("lines1", TestData.getTextData());
		inPipeDataMap.put("lines2", TestData.getTextData2());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {
		return Collections.singleton("joined");
	}

	@Override
	public FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );

		Pipe linePipe1 = new Pipe("lines1");
		RegexSplitGenerator splitter1 = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		Pipe wordsPipe1 = new Each(linePipe1, text, splitter1, Fields.RESULTS );

		Pipe linePipe2 = new Pipe("lines2");
		RegexSplitGenerator splitter2 = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		Pipe wordsPipe2 = new Each(linePipe2, text, splitter2, Fields.RESULTS );

		CoGroup joinedLinesPipe = new CoGroup(
				"joined",
				new Pipe[]{wordsPipe1, wordsPipe2},
				new Fields[]{token, token},
				new Fields("token1", "token2"),
				new RightJoin());


		FlowDef flowDef = FlowDef.flowDef().setName( "joined" )
				.addTail(joinedLinesPipe);

		return flowDef;
	}
}
