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

package com.dataArtisans.flinkCascading.util;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.dataArtisans.flinkCascading.planning.FlinkConnector;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class FlinkCascadingTestBase {

	private Map<String, String> inPipePathMap;
	private Map<String, String> localOutPipePathMap;
	private Map<String, String> flinkOutPipePathMap;

	public abstract Map<String, String> getInPipeDataMap();

	public abstract Set<String> getOutPipes();

	public abstract FlowDef getFlow();

	@Before
	public void setup() throws IOException {

		Map<String, String> inPipeDataMap = getInPipeDataMap();
		this.inPipePathMap = new HashMap<String, String>();

		for(String inPipe : inPipeDataMap.keySet()) {
			String data = inPipeDataMap.get(inPipe);
			File tmpInputFile = File.createTempFile("flinkCascading-in", ".tmp");

			BufferedWriter bw = new BufferedWriter(new FileWriter(tmpInputFile));
			bw.write(data);
			bw.flush();
			bw.close();

			inPipePathMap.put(inPipe, tmpInputFile.getAbsolutePath());
		}

		Set<String> outPipes = getOutPipes();

		this.localOutPipePathMap = new HashMap<String, String>();
		this.flinkOutPipePathMap = new HashMap<String, String>();

		for(String outPipe : outPipes) {
			File localOutputFile = File.createTempFile("flinkCascading-localOut", ".tmp");
			File flinkOutputFile = File.createTempFile("flinkCascading-flinkOut", ".tmp");
			flinkOutputFile.delete();

			localOutPipePathMap.put(outPipe, localOutputFile.getAbsolutePath());
			flinkOutPipePathMap.put(outPipe, flinkOutputFile.getAbsolutePath());
		}

	}

	@Test
	public void testFlow() {

		try {
			this.runLocalFlow();
			this.runFlinkFlow();

			Set<String> outPipes = getOutPipes();
			for (String outPipe : outPipes) {
				String localPath = this.localOutPipePathMap.get(outPipe);
				String flinkPath = this.flinkOutPipePathMap.get(outPipe);

				compareOutputs(outPipe, localPath, flinkPath);
			}
		} catch(IOException e) {
			e.printStackTrace();
			fail(); // test error?
		} catch(Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@After
	public void cleanup() {

		for(String path : this.inPipePathMap.values()) {
			File inFile = new File(path);
			inFile.delete();
		}

		for(String path : this.localOutPipePathMap.values()) {
			File inFile = new File(path);
			inFile.delete();
		}

		for(String path : this.flinkOutPipePathMap.values()) {
			File inFile = new File(path);
			inFile.delete();
		}
	}

	private void runFlinkFlow() {

		FlowDef flinkFlow = getFlinkCascadingFlow();

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		FlinkConnector fc = new FlinkConnector(env);
		fc.connect(flinkFlow).complete();
	}

	private void runLocalFlow() {

		FlowDef localFlow = getLocalCascadingFlow();

		LocalFlowConnector lfc = new LocalFlowConnector();
		lfc.connect(localFlow).complete();
	}

	private FlowDef getLocalCascadingFlow() {

		FlowDef flow = this.getFlow();

		for(String inPipe: inPipePathMap.keySet()) {
			String path = inPipePathMap.get(inPipe);
			Tap sourceTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextLine(), path);

			flow.addSource(inPipe, sourceTap);
		}

		for(String outPipe: localOutPipePathMap.keySet()) {
			String path = localOutPipePathMap.get(outPipe);
//			Tap sinkTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextLine(new Fields("num, line"), new Fields("count", "ip")), path);
			Tap sinkTap = new cascading.tap.local.FileTap(new cascading.scheme.local.TextLine(), path);

			flow.addSink(outPipe, sinkTap);
		}

		return flow;
	}

	private FlowDef getFlinkCascadingFlow() {

		FlowDef flow = this.getFlow();

		for(String inPipe: inPipePathMap.keySet()) {
			String path = inPipePathMap.get(inPipe);
			Tap sourceTap = new FileTap(new cascading.scheme.local.TextLine(), path);

			flow.addSource(inPipe, sourceTap);
		}

		for(String outPipe: flinkOutPipePathMap.keySet()) {
			String path = flinkOutPipePathMap.get(outPipe);
//			Tap sinkTap = new FileTap(new cascading.scheme.local.TextLine(new Fields("num, line"), new Fields("count", "ip")), path);
			Tap sinkTap = new FileTap(new cascading.scheme.local.TextLine(), path);

			flow.addSink(outPipe, sinkTap);
		}

		return flow;
	}

	private void compareOutputs(String pipe, String localPath, String flinkPath) throws IOException {

		List<String> localLines = readLines(localPath);
		List<String> flinkLines = readLines(flinkPath);

		assertEquals("Cascading Local and Cascading Flink compute results of different sizes for pipe "+pipe+": "
						+localLines.size()+" vs. "+flinkLines.size(),
				localLines.size(), flinkLines.size());

		Collections.sort(localLines);
		Collections.sort(flinkLines);

		for(int i=0; i<localLines.size(); i++) {
			assertEquals("Cascading Local and Cascading Flink compute different result for pipe "+pipe+": \""
					+localLines.get(i)+"\" vs. \""+flinkLines.get(i)+"\"",
				localLines.get(i), flinkLines.get(i));
		}
	}

	private List<String> readLines(String path) throws IOException {
		ArrayList<String> lines = new ArrayList<String>();

		File file = new File(path);
		BufferedReader br = new BufferedReader(new FileReader(file));

		while(br.ready()) {
			lines.add(br.readLine());
		}

		return lines;
	}

}
