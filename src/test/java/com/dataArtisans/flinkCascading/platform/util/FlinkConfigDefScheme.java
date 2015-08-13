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

package com.dataArtisans.flinkCascading.platform.util;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class FlinkConfigDefScheme extends TextLine {

	public FlinkConfigDefScheme(Fields sourceFields) {
		super(sourceFields);
	}

	@Override
	public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf ) {
		super.sourceConfInit( flowProcess, tap, conf );
	}

	@Override
	public void sinkConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf ) {
		super.sinkConfInit( flowProcess, tap, conf );
	}

	@Override
	public void sourcePrepare( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) {
		if( !( flowProcess instanceof FlowProcessWrapper) ) {
			throw new RuntimeException( "not a flow process wrapper" );
		}

		if( !"process-default".equals( flowProcess.getProperty( "default" ) ) ) {
			throw new RuntimeException("not default value");
		}

		if( !"source-replace".equals( flowProcess.getProperty( "replace" ) ) ) {
			throw new RuntimeException( "not replaced value" );
		}

		if( !"node-replace".equals( flowProcess.getProperty( "default-node" ) ) ) {
			throw new RuntimeException( "not replaced value" );
		}

		flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

		if( !"process-default".equals( flowProcess.getProperty( "default" ) ) ) {
			throw new RuntimeException( "not default value" );
		}

		if( !"process-replace".equals( flowProcess.getProperty( "replace" ) ) ) {
			throw new RuntimeException( "not replaced value" );
		}

		super.sourcePrepare( flowProcess, sourceCall );
	}

	@Override
	public void sinkPrepare( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException {
		if( !( flowProcess instanceof FlowProcessWrapper ) ) {
			throw new RuntimeException( "not a flow process wrapper" );
		}

		if( !"process-default".equals( flowProcess.getProperty( "default" ) ) ) {
			throw new RuntimeException( "not default value" );
		}

		if( !"sink-replace".equals( flowProcess.getProperty( "replace" ) ) ) {
			throw new RuntimeException( "not replaced value" );
		}

		flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

		if( !"process-default".equals( flowProcess.getProperty( "default" ) ) ) {
			throw new RuntimeException( "not default value" );
		}

		if( !"process-replace".equals( flowProcess.getProperty( "replace" ) ) ) {
			throw new RuntimeException( "not replaced value" );
		}

		super.sinkPrepare( flowProcess, sinkCall );
	}

}
