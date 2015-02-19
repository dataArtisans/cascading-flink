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

package com.dataArtisans.flinkCascading.runners;

import cascading.flow.FlowDef;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.dataArtisans.flinkCascading.flows.MultiAggregateFlow;
import com.dataArtisans.flinkCascading.flows.TokenizeFlow;
import com.dataArtisans.flinkCascading.flows.WordCountFlow;
import com.dataArtisans.flinkCascading.planning.FlinkConnector;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkFlowRunner {

	public static void main(String[] args) throws Exception {


		Tap docTap = new Hfs(new TextLine(), "file:///users/fhueske/testFile");
		Tap wcTap = new Hfs(new TextLine(), "file:///users/fhueske/wcResult");

//		Tap docTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextLine(), "/users/fhueske/testFile");
//		Tap wcTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextLine(), "/users/fhueske/wcResult");

		FlowDef tokenizeFlow = TokenizeFlow.getTokenizeFlow()
				.addSource( "token", docTap )
				.addSink("token", wcTap);

		FlowDef wcFlow = WordCountFlow.getWordCountFlow()
				.addSource( "token", docTap )
				.addSink("wc", wcTap);

		FlowDef aggFlow = MultiAggregateFlow.getFlow()
				.addSource( "token", docTap )
				.addSink("wc", wcTap);


//		cascading.flow.local.LocalFlowConnector lfc = new cascading.flow.local.LocalFlowConnector();
//		lfc.connect(aggFlow).complete();

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		FlinkConnector fc = new FlinkConnector(env);
		fc.connect(wcFlow).complete();
//		fc.connect(aggFlow).complete();

	}


}
