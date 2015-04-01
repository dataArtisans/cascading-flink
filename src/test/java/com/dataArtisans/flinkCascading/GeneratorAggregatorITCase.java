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
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.util.FlinkCascadingTestBase;
import com.dataArtisans.flinkCascading.util.TestData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class GeneratorAggregatorITCase extends FlinkCascadingTestBase {

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

		pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

		pipe = new GroupBy( pipe, new Fields( "ip" ) );

		pipe = new Every( pipe, new TestAggregator( new Fields( "count1" ), new Fields( "ip" ), new Tuple( "first1" ), new Tuple( "first2" ) ) );
		pipe = new Every( pipe, new TestAggregator( new Fields( "count2" ), new Fields( "ip" ), new Tuple( "second" ), new Tuple( "second2" ), new Tuple( "second3" ) ) );

		FlowDef flowDef = FlowDef.flowDef().addTails(pipe);

		return flowDef;
	}

	public static class TestAggregator extends BaseOperation implements Aggregator
	{
		private static final long serialVersionUID = 1L;
		private Tuple[] value;
		private int duplicates = 1;
		private Fields groupFields;

		/**
		 * Constructor
		 *
		 * @param fields the fields to operate on
		 * @param value
		 */
		public TestAggregator( Fields fields, Tuple... value )
		{
			super( fields );
			this.value = value;
		}

		public TestAggregator( Fields fields, Fields groupFields, Tuple... value )
		{
			super( fields );
			this.groupFields = groupFields;
			this.value = value;
		}

		/**
		 * Constructor TestAggregator creates a new TestAggregator instance.
		 *
		 * @param fieldDeclaration of type Fields
		 * @param value            of type Tuple
		 * @param duplicates       of type int
		 */
		public TestAggregator( Fields fieldDeclaration, Tuple value, int duplicates )
		{
			super( fieldDeclaration );
			this.value = new Tuple[]{value};
			this.duplicates = duplicates;
		}

		public TestAggregator( Fields fieldDeclaration, Fields groupFields, Tuple value, int duplicates )
		{
			super( fieldDeclaration );
			this.groupFields = groupFields;
			this.value = new Tuple[]{value};
			this.duplicates = duplicates;
		}

		public void start( FlowProcess flowProcess, AggregatorCall aggregatorCall )
		{
			if( groupFields == null )
				return;

			if( !groupFields.equals( aggregatorCall.getGroup().getFields() ) )
				throw new RuntimeException( "fields do not match: " + groupFields.print() + " != " + aggregatorCall.getGroup().getFields().print() );
		}

		public void aggregate( FlowProcess flowProcess, AggregatorCall aggregatorCall )
		{
		}

		public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
		{
			for( int i = 0; i < duplicates; i++ )
			{
				for( Tuple tuple : value )
					aggregatorCall.getOutputCollector().add( tuple );
			}
		}
	}

}
