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

import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuple;
import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormatBase;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import static cascading.flow.hadoop.util.HadoopUtil.asJobConfInstance;

public class HfsInputFormat implements InputFormat<Tuple, HadoopInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatBase.class);


	private Hfs hfsTap;
	private transient HadoopFlowProcess ffp;
	private transient TupleEntryIterator it;

	private transient org.apache.hadoop.mapred.InputFormat<? extends WritableComparable, ? extends Writable> mapredInputFormat;
	private transient JobConf jobConf;

	private transient boolean fetched = false;
	private transient boolean hasNext;
	private transient Tuple next;

	public HfsInputFormat(Hfs hfsTap, org.apache.hadoop.conf.Configuration config) {
		super();
		this.hfsTap = hfsTap;
		this.jobConf = asJobConfInstance( config );
	}
	

	// --------------------------------------------------------------------------------------------
	//  InputFormat
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void configure(Configuration parameters) {

		// get Hadoop mapReduce InputFormat
//		org.apache.hadoop.conf.Configuration conf = ffp.getConfigCopy();

		// prevent collisions of configuration properties set client side if now cluster side
//		String property = ffp.getStringProperty( "cascading.node.accumulated.source.conf." + Tap.id(hfsTap) );
//
//		if( property == null )
//		{
//			// default behavior is to accumulate paths, so remove any set prior
//			conf = HadoopUtil.removePropertiesFrom(conf, "mapred.input.dir", "mapreduce.input.fileinputformat.inputdir"); // hadoop2
//			hfsTap.sourceConfInit( ffp, conf );
//		}
		this.ffp = new HadoopFlowProcess(jobConf);

		this.mapredInputFormat = jobConf.getInputFormat();

		if( this.mapredInputFormat instanceof JobConfigurable) {
			((JobConfigurable) this.mapredInputFormat).configure(jobConf);
		}

	}
	
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// only gather base statistics for FileInputFormats
		if(!(mapredInputFormat instanceof FileInputFormat)) {
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
		for(int i=0;i<splitArray.length;i++){
			hiSplit[i] = new HadoopInputSplit(i, splitArray[i], jobConf);
		}
		return hiSplit;
	}
	
	@Override
	public InputSplitAssigner getInputSplitAssigner(HadoopInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}
	
	@Override
	public void open(HadoopInputSplit split) throws IOException {
		RecordReader<?,?> recordReader = this.mapredInputFormat.getRecordReader(split.getHadoopInputSplit(), jobConf, new HadoopDummyReporter());

//		if (this.recordReader instanceof Configurable) {
//			((Configurable) this.recordReader).setConf(jobConf);
//		}

		this.it = hfsTap.openForRead(this.ffp, recordReader);
		this.fetched = false;
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		if(!fetched) {
			fetchNext();
		}
		return !hasNext;
	}
	
	protected void fetchNext() throws IOException {
		if(this.it.hasNext()) {
			this.hasNext = true;
			this.next = this.it.next().getTuple();
			this.fetched = true;
			this.ffp.increment( StepCounters.Tuples_Read, 1 );
			this.ffp.increment(SliceCounters.Tuples_Read, 1);
		} else {
			this.hasNext = false;
		}
	}

	@Override
	public Tuple nextRecord(Tuple record) throws IOException {
		if(!fetched) {
			fetchNext();
		}
		if(!hasNext) {
			return null;
		}
		fetched = false;
		return next;
	}

	@Override
	public void close() throws IOException {
		this.it.close();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------
	
	private FileBaseStatistics getFileStats(FileBaseStatistics cachedStats, org.apache.hadoop.fs.Path[] hadoopFilePaths,
			ArrayList<FileStatus> files) throws IOException {
		
		long latestModTime = 0L;
		
		// get the file info and check whether the cached statistics are still valid.
		for(org.apache.hadoop.fs.Path hadoopPath : hadoopFilePaths) {
			
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

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeObject(this.hfsTap);
		jobConf.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.hfsTap = (Hfs)in.readObject();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
	}

}
