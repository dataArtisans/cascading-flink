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

import cascading.CascadingException;
import cascading.flow.FlowNode;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.TrapHandler;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class HfsOutputFormat implements OutputFormat<Tuple> {

	private static final long serialVersionUID = 1L;

	private FlowNode node;

	private Hfs hfsTap;
	private Fields tapFields;
	private Hfs trap;

	private transient FlinkFlowProcess flowProcess;
	private transient TupleEntryCollector tupleEntryCollector;
	private transient TrapHandler trapHandler;

	private transient JobConf jobConf;


	public HfsOutputFormat(Hfs hfs, Fields tapFields, FlowNode node) {
		super();

		this.node = node;

		this.hfsTap = hfs;
		this.tapFields = tapFields;

		// check if there is at most one trap
		if(node.getTraps().size() > 1) {
			throw new IllegalArgumentException("At most one trap allowed for data source");
		}
		if(node.getTraps().size() > 0) {
			// check if trap is Hfs
			if (!(node.getTraps().iterator().next() instanceof Hfs)) {
				throw new IllegalArgumentException("Trap must be of type Hfs");
			}
			this.trap = (Hfs) node.getTraps().iterator().next();
		}
		else {
			this.trap = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  OutputFormat
	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {
		// do nothing

		this.jobConf = HadoopUtil.asJobConfInstance(FlinkConfigConverter.toHadoopConfig(parameters));

		FakeRuntimeContext rc = new FakeRuntimeContext();
		rc.setName("Sink-"+this.node.getID());
		rc.setTaskNum(1);

		this.flowProcess = new FlinkFlowProcess(jobConf, rc);

		this.trapHandler = new TrapHandler(flowProcess, this.hfsTap, this.trap, "MyFunkyName"); // TODO set name

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
		try {
			this.tupleEntryCollector.add(t);
		}
		catch (OutOfMemoryError error) {
			handleReThrowableException("out of memory, try increasing task memory allocation", error);
		} catch (CascadingException exception) {
			handleException(exception, null);
		} catch (Throwable throwable) {
			handleException(new DuctException("internal error", throwable), null);
		}
	}

	/**
	 * @throws java.io.IOException
	 */
	@Override
	public void close() throws IOException {
		this.tupleEntryCollector.close();
		flowProcess.closeTrapCollectors();
	}

	protected void handleReThrowableException(String message, Throwable throwable) {
		this.trapHandler.handleReThrowableException( message, throwable );
	}

	protected void handleException(Throwable exception, TupleEntry tupleEntry) {
		this.trapHandler.handleException( exception, tupleEntry );
	}

}
