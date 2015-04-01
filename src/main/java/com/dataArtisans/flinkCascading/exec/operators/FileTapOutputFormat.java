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

package com.dataArtisans.flinkCascading.exec.operators;

import cascading.flow.local.LocalFlowProcess;
import cascading.tap.local.FileTap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

public class FileTapOutputFormat implements OutputFormat<Tuple> { // , FinalizeOnMaster {

	private static final long serialVersionUID = 1L;

	private FileTap fileTap;
	private Properties props;

	private transient TupleEntryCollector tupleEntryCollector;


	public FileTapOutputFormat(FileTap fileTap, Properties props) {
		super();
		this.fileTap = fileTap;
		this.props = props;
	}

	// --------------------------------------------------------------------------------------------
	//  OutputFormat
	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {
		// do nothing
	}

	/**
	 * create the temporary output file for hadoop RecordWriter.
	 * @param taskNumber The number of the parallel instance.
	 * @param numTasks The number of parallel tasks.
	 * @throws java.io.IOException
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		LocalFlowProcess fp = new LocalFlowProcess();

		this.tupleEntryCollector = this.fileTap.openForWrite(fp, null);
	}

	@Override
	public void writeRecord(Tuple t) throws IOException {
		this.tupleEntryCollector.add(t);
	}

	/**
	 * @throws java.io.IOException
	 */
	@Override
	public void close() throws IOException {
		this.tupleEntryCollector.close();
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeObject(this.fileTap);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.fileTap = (FileTap)in.readObject();
	}

}
