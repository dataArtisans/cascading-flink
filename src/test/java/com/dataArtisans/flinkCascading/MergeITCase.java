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
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class MergeITCase extends FlinkCascadingTestBase {

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

		Pipe linePipe1 = new Pipe("lines1");
		Pipe linePipe2 = new Pipe("lines2");
		Pipe linePipe3 = new Pipe("lines3");
		Merge mergedLinesPipe = new Merge("mergedLines", linePipe1, linePipe2, linePipe3);

		Pipe wordsPipe = new Pipe("words", mergedLinesPipe);
		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		wordsPipe = new Each(wordsPipe, text, splitter, Fields.RESULTS );

		Pipe countsPipe = new Pipe( "counts", wordsPipe );
		countsPipe = new GroupBy( countsPipe, token );
		countsPipe = new Every( countsPipe, Fields.ALL, new Count(), Fields.ALL );

		FlowDef flowDef = FlowDef.flowDef().setName( "counts" )
				.addTail(countsPipe);

		return flowDef;
	}
}
