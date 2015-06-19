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
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ChainedAggregatorITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(1);
		inPipeDataMap.put("token", TestData.getTextData());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {
		return Collections.singleton("wc");
	}

	@Override
	public FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );
		Fields num = new Fields( "num" );
		Fields sum = new Fields( "sum" );
		Fields avg = new Fields( "avg" );

		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		// only returns "token"
		Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );
		docPipe = new Each( docPipe, Fields.NONE, new Extender(num), Fields.ALL);

		Pipe wcPipe = new Pipe( "wc", docPipe );
		wcPipe = new GroupBy( wcPipe, token );
		wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );
		wcPipe = new Every( wcPipe, num, new Sum(sum), Fields.ALL );
		wcPipe = new Every( wcPipe, num, new Average(avg), Fields.ALL );

		FlowDef flowDef = FlowDef.flowDef().setName( "wc" )
				.addTail(wcPipe);

		return flowDef;
	}

	public static class Extender implements Function, Serializable {

		Fields fields;
		Tuple t;

		public Extender() {}

		public Extender(Fields f) {
			fields = f;
			t = new Tuple();
			t.add(42);
		}

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			functionCall.getOutputCollector().add(t);
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
			return this.fields;
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
