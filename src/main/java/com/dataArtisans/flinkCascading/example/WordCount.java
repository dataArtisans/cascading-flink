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

package com.dataArtisans.flinkCascading.example;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.FlinkConnector;


public class WordCount {
	public static void main(String[] args) {

		if (args.length < 2) {
			throw new IllegalArgumentException("Please specify input and ouput paths as arguments.");
		}

		Fields token = new Fields( "token" );
		Fields text = new Fields( "text" );
		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "\\s+" );
		// only returns "token"
		Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

		Pipe wcPipe = new Pipe( "wc", docPipe );
		wcPipe = new AggregateBy( wcPipe, token, new CountBy(new Fields("count")));

		Tap inTap = new Hfs(new TextDelimited(text, "\n" ), args[0]);
		Tap outTap = new Hfs(new TextDelimited(false, "\n"), args[1]);

		FlowDef flowDef = FlowDef.flowDef().setName( "wc" )
				.addSource( docPipe, inTap )
				.addTailSink( wcPipe, outTap );

		FlowConnector flowConnector = new FlinkConnector();

		Flow wcFlow = flowConnector.connect( flowDef );

		wcFlow.complete();
	}

}
