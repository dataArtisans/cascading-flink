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
import cascading.operation.aggregator.Count;
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


public class MergingCoGroupITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(3);
		inPipeDataMap.put("lines1", TestData.getTextData());
		inPipeDataMap.put("lines2", TestData.getTextData2());
		inPipeDataMap.put("lines3", TestData.getTextData3());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {
		return Collections.singleton("counts");
	}

	@Override
	public FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );

		Pipe linesPipe1 = new Pipe("lines1");
		Pipe wordsPipe1 = new Pipe("words1", linesPipe1);
		RegexSplitGenerator splitter1 = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		wordsPipe1 = new Each(wordsPipe1, text, splitter1, Fields.RESULTS );

		Pipe linesPipe2 = new Pipe("lines2");
		Pipe wordsPipe2 = new Pipe("words2", linesPipe2);
		RegexSplitGenerator splitter2 = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		wordsPipe2 = new Each(wordsPipe2, text, splitter2, Fields.RESULTS );

		Pipe linesPipe3 = new Pipe("lines3");
		Pipe wordsPipe3 = new Pipe("words3", linesPipe3);
		RegexSplitGenerator splitter3 = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		wordsPipe3 = new Each(wordsPipe3, text, splitter3, Fields.RESULTS );

		Pipe countsPipe = new GroupBy( "counts", new Pipe[]{wordsPipe1, wordsPipe2, wordsPipe3}, token );
		countsPipe = new Every( countsPipe, Fields.ALL, new Count(), Fields.ALL );

		FlowDef flowDef = FlowDef.flowDef().setName( "counts" )
				.addTail(countsPipe);

		return flowDef;
	}
}
