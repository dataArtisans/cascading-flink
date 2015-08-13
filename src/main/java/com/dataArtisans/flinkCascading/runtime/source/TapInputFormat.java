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


package com.dataArtisans.flinkCascading.runtime.source;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.runtime.util.FlinkFlowProcess;
import com.dataArtisans.flinkCascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static cascading.util.LogUtil.logCounters;
import static cascading.util.LogUtil.logMemory;

public class TapInputFormat extends RichInputFormat<Tuple, HadoopInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(TapInputFormat.class);

	private FlowNode flowNode;

	private transient SourceStreamGraph streamGraph;
	private transient TapSourceStage sourceStage;
	private transient SingleOutBoundaryStage sinkStage;

	private transient FlowProcess flowProcess;
	private transient long processBeginTime;

	private transient org.apache.hadoop.mapred.InputFormat<? extends WritableComparable, ? extends Writable> mapredInputFormat;
	private transient JobConf jobConf;

	public TapInputFormat(FlowNode flowNode) {

		super();
		this.flowNode = flowNode;

	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Configuration config) {

		this.jobConf = HadoopUtil.asJobConfInstance(FlinkConfigConverter.toHadoopConfig(config));

		// set the correct class loader
		// not necessary for Flink versions >= 0.10 but we set this anyway to be on the safe side
		jobConf.setClassLoader(this.getClass().getClassLoader());

		this.mapredInputFormat = jobConf.getInputFormat();

		if (this.mapredInputFormat instanceof JobConfigurable) {
			((JobConfigurable) this.mapredInputFormat).configure(jobConf);
		}
	}



	@Override
	public void open(HadoopInputSplit split) throws IOException {

		this.jobConf = split.getJobConf();
		this.flowProcess = new FlinkFlowProcess(this.jobConf, this.getRuntimeContext(), flowNode.getID());

		processBeginTime = System.currentTimeMillis();
		flowProcess.increment( SliceCounters.Process_Begin_Time, processBeginTime );

		try {

			Set<FlowElement> sources = flowNode.getSourceElements();
			if(sources.size() != 1) {
				throw new RuntimeException("FlowNode for TapInputFormat may only have a single source");
			}
			FlowElement sourceElement = sources.iterator().next();
			if(!(sourceElement instanceof Tap)) {
				throw new RuntimeException("Source of TapInputFormat must be a Tap");
			}
			Tap source = (Tap)sourceElement;

			streamGraph = new SourceStreamGraph( flowProcess, flowNode, source );

			sourceStage = this.streamGraph.getSourceStage();
			sinkStage = this.streamGraph.getSinkStage();

			for( Duct head : streamGraph.getHeads() ) {
				LOG.info("sourcing from: " + ((ElementDuct) head).getFlowElement());
			}

			for( Duct tail : streamGraph.getTails() ) {
				LOG.info("sinking to: " + ((ElementDuct) tail).getFlowElement());
			}

		}
		catch( Throwable throwable ) {

			if( throwable instanceof CascadingException) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during TapInputFormat configuration", throwable );
		}

		RecordReader<?, ?> recordReader = this.mapredInputFormat.getRecordReader(split.getHadoopInputSplit(), jobConf, new HadoopDummyReporter());

		if (recordReader instanceof Configurable) {
			((Configurable) recordReader).setConf(jobConf);
		}
		else if (recordReader instanceof JobConfigurable) {
			((JobConfigurable) recordReader).configure(jobConf);
		}

		try {
			this.sourceStage.setRecordReader(recordReader);
		} catch(Throwable t) {
			if(t instanceof IOException) {
				throw (IOException)t;
			}
			else {
				throw new RuntimeException(t);
			}
		}

	}

	@Override
	public boolean reachedEnd() throws IOException {

		try {
			return !sinkStage.hasNextTuple() && !this.sourceStage.readNextRecord();
		}
		catch( OutOfMemoryError error ) {
			throw error;
		}
		catch( IOException exception ) {
			throw exception;
		}
		catch( Throwable throwable ) {
			if( throwable instanceof CascadingException ) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during TapInputFormat execution", throwable );
		}
	}

	@Override
	public Tuple nextRecord(Tuple record) throws IOException {

		if(this.reachedEnd()) {
			return null;
		}
		else {
			return sinkStage.fetchNextTuple();
		}
	}

	@Override
	public void close() throws IOException {
		try {
			streamGraph.cleanup();
		}
		finally {

			long processEndTime = System.currentTimeMillis();
			flowProcess.increment(SliceCounters.Process_End_Time, processEndTime);
			flowProcess.increment( SliceCounters.Process_Duration, processEndTime - this.processBeginTime );

			String message = "flow node id: " + flowNode.getID();
			logMemory( LOG, message + ", mem on close" );
			logCounters( LOG, message + ", counter:", flowProcess );
		}
	}


	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// only gather base statistics for FileInputFormats
		if (!(mapredInputFormat instanceof FileInputFormat)) {
			return null;
		}

		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
				(FileBaseStatistics) cachedStats : null;

		try {
			final org.apache.hadoop.fs.Path[] paths = FileInputFormat.getInputPaths(this.jobConf);

			return getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Could not determine statistics due to an io error: "
						+ ioex.getMessage());
			}
		} catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Unexpected problem while getting the file statistics: "
						+ t.getMessage(), t);
			}
		}

		// no statistics available
		return null;
	}

	@Override
	public HadoopInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {

		org.apache.hadoop.mapred.InputSplit[] splitArray = mapredInputFormat.getSplits(jobConf, minNumSplits);
		HadoopInputSplit[] hiSplit = new HadoopInputSplit[splitArray.length];
		for (int i = 0; i < splitArray.length; i++) {
			hiSplit[i] = new HadoopInputSplit(i, splitArray[i], jobConf);
		}
		return hiSplit;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HadoopInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	private FileBaseStatistics getFileStats(FileBaseStatistics cachedStats, org.apache.hadoop.fs.Path[] hadoopFilePaths,
											ArrayList<FileStatus> files) throws IOException {

		long latestModTime = 0L;

		// get the file info and check whether the cached statistics are still valid.
		for (org.apache.hadoop.fs.Path hadoopPath : hadoopFilePaths) {

			final Path filePath = new Path(hadoopPath.toUri());
			final FileSystem fs = FileSystem.get(filePath.toUri());

			final FileStatus file = fs.getFileStatus(filePath);
			latestModTime = Math.max(latestModTime, file.getModificationTime());

			// enumerate all files and check their modification time stamp.
			if (file.isDir()) {
				FileStatus[] fss = fs.listStatus(filePath);
				files.ensureCapacity(files.size() + fss.length);

				for (FileStatus s : fss) {
					if (!s.isDir()) {
						files.add(s);
						latestModTime = Math.max(s.getModificationTime(), latestModTime);
					}
				}
			} else {
				files.add(file);
			}
		}

		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}

		// calculate the whole length
		long len = 0;
		for (FileStatus s : files) {
			len += s.getLen();
		}

		// sanity check
		if (len <= 0) {
			len = BaseStatistics.SIZE_UNKNOWN;
		}

		return new FileBaseStatistics(latestModTime, len, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}

}
