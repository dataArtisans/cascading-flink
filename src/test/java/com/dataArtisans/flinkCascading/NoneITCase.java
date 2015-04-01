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
import cascading.operation.Function;
import cascading.operation.Insert;
import cascading.operation.NoOp;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class NoneITCase extends FlinkCascadingTestBase {

	@Override
	public Map<String, String> getInPipeDataMap() {
		Map<String, String> inPipeDataMap = new HashMap<String, String>(1);
		inPipeDataMap.put("test", TestData.getApacheLogData());

		return inPipeDataMap;
	}

	@Override
	public Set<String> getOutPipes() {

		Set<String> outPipes = new HashSet<String>();
		outPipes.add("test");
		return outPipes;
	}

	@Override
	public FlowDef getFlow() {

		Pipe pipe = new Pipe( "test" );

		Function parser = new RegexParser( new Fields( "ip" ), "^[^ ]*" );
		pipe = new Each( pipe, new Fields( "line" ), parser, Fields.ALL );
		pipe = new Each( pipe, new Fields( "line" ), new NoOp(), Fields.SWAP ); // declares Fields.NONE
		pipe = new GroupBy( pipe, new Fields( "ip" ) );
		pipe = new Every( pipe, new Fields( "ip" ), new Count( new Fields( "count" ) ) );
		pipe = new Each( pipe, Fields.NONE, new Insert( new Fields( "ipaddress" ), "1.2.3.4" ), Fields.ALL );

		FlowDef flowDef = FlowDef.flowDef().addTails(pipe);

		return flowDef;
	}


}
