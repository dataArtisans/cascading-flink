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
import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class TokenizeFilterITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(1);
		inPipeDataMap.put("words", TestData.getTextData());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {
		return Collections.singleton("words");
	}

	@Override
	public FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );

		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		// only returns "token"
		Pipe wordsPipe = new Each( "words", text, splitter, Fields.RESULTS );
		wordsPipe = new Each(wordsPipe, token, new TokenFilter());

		FlowDef flowDef = FlowDef.flowDef().setName( "words" )
				.addTail(wordsPipe);

		return flowDef;
	}

	public static class TokenFilter implements Filter, Serializable {

		@Override
		public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
			return filterCall.getArguments().getString("token").length() > 4;
		}

		@Override
		public void prepare(FlowProcess flowProcess, OperationCall operationCall) {

		}

		@Override
		public void flush(FlowProcess flowProcess, OperationCall operationCall) {

		}

		@Override
		public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {

		}

		@Override
		public Fields getFieldDeclaration() {
			return Fields.ALL;
		}

		@Override
		public int getNumArgs() {
			return 0;
		}

		@Override
		public boolean isSafe() {
			return false;
		}
	}
}
