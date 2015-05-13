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

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static cascading.flow.hadoop.util.HadoopUtil.asJobConfInstance;

public class HfsOutputFormat implements OutputFormat<Tuple> { // , FinalizeOnMaster {

	private static final long serialVersionUID = 1L;

	private Hfs hfsTap;
	private Fields tapFields;
	private JobConf jobConf;

	private transient TupleEntryCollector tupleEntryCollector;


	public HfsOutputFormat(Hfs hfs, Fields tapFields, org.apache.hadoop.conf.Configuration config) {
		super();
		this.hfsTap = hfs;
		this.tapFields = tapFields;
		this.jobConf = asJobConfInstance( config );
	}

	public JobConf getJobConf() {
		return jobConf;
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

		this.jobConf.setInt("mapred.task.partition", taskNumber + 1);
		this.jobConf.setNumMapTasks(numTasks);

		HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);

		this.tupleEntryCollector = this.hfsTap.openForWrite(hfp, null);
		if( this.hfsTap.getSinkFields().isAll() )
		{
			this.tupleEntryCollector.setFields(tapFields);
		}
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

//	@Override
//	public void finalizeGlobal(int parallelism) throws IOException {
//
//		try {
//			JobContext jobContext = HadoopUtils.instantiateJobContext(this.jobConf, new JobID());
//			FileOutputCommitter fileOutputCommitter = new FileOutputCommitter();
//
//			// finalize HDFS output format
//			fileOutputCommitter.commitJob(jobContext);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeObject(this.hfsTap);
		out.writeObject(this.tapFields);
		jobConf.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.hfsTap = (Hfs)in.readObject();
		this.tapFields = (Fields)in.readObject();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
	}

}
