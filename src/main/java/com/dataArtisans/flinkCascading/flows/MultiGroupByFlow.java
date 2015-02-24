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

package com.dataArtisans.flinkCascading.flows;

import cascading.flow.FlowDef;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class MultiGroupByFlow {

	public static FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );

		Pipe linePipe1 = new Pipe("line1");
		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		Pipe docPipe1 = new Each( linePipe1, text, splitter, Fields.RESULTS );

		Pipe linePipe2 = new Pipe("line2");
		RegexSplitGenerator splitter2 = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		Pipe docPipe2 = new Each( linePipe2, text, splitter2, Fields.RESULTS );

		Pipe wcPipe = new GroupBy( "wc", docPipe1, docPipe2, token );
		wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

		FlowDef flowDef = FlowDef.flowDef().setName( "wc" )
				.addTail(wcPipe);

		return flowDef;
	}

}
