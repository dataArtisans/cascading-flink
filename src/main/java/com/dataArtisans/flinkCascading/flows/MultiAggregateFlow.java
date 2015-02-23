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
import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.buffer.FirstNBuffer;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.Serializable;

public class MultiAggregateFlow {

	public static FlowDef getFlow() {

		Fields token = new Fields( "token" );
		Fields text = new Fields( "line" );
		Fields offset = new Fields( "offset" );
		Fields num = new Fields( "num" );
		Fields cnt = new Fields( "count" );
		Fields blubb = new Fields( "blubb" );

		RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
		// only returns "token"
		Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );
		docPipe = new Each( docPipe, new Extender( blubb), Fields.ALL);

		Pipe wcPipe = new Pipe( "wc", docPipe );
		wcPipe = new GroupBy( wcPipe, token );
//		wcPipe = new Each( wcPipe, num, new DoubleFunc(offset), Fields.ALL);
		wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );
		wcPipe = new Every( wcPipe, blubb, new Sum(num), Fields.ALL );
		wcPipe = new Each( wcPipe, token, new TokenFilter());
		wcPipe = new GroupBy( wcPipe, cnt);
		wcPipe = new Every( wcPipe, cnt, new FirstNBuffer(2), Fields.REPLACE);

		return FlowDef.flowDef().setName( "wc" )
				.addTails(wcPipe);
	}

	public static class DoubleFunc implements Function {

		Fields fields;

		public DoubleFunc(Fields f) {
			fields = f;
		}

		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry x = functionCall.getArguments();
			long newNum = x.getLong(0)*2;
			Tuple t = new Tuple(newNum);
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
			return 1;
		}

		@Override
		public boolean isSafe() {
			return false;
		}
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
			return 1;
		}

		@Override
		public boolean isSafe() {
			return false;
		}
	}

	public static class TokenFilter implements Filter, Serializable {

		@Override
		public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
			return filterCall.getArguments().getString("token").length() > 5;
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
