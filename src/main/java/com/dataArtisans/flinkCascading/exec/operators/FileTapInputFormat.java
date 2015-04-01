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
import cascading.tuple.TupleEntryIterator;
import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormatBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Properties;


public class FileTapInputFormat implements InputFormat<Tuple, FileInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatBase.class);


	private FileTap fileTap;
	private transient LocalFlowProcess fp;
	private transient TupleEntryIterator it;

	private transient boolean fetched = false;
	private transient boolean hasNext;
	private transient Tuple next;

	public FileTapInputFormat(FileTap fileTap, Properties config) {
		super();
		this.fileTap = fileTap;
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
		this.fp = new LocalFlowProcess();

	}
	
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// only gather base statistics for FileInputFormats

		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
				(FileBaseStatistics) cachedStats : null;
		
//		try {
//			final org.apache.hadoop.fs.Path[] paths = FileInputFormat.getInputPaths(this.jobConf);
//
//			return getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
//		} catch (IOException ioex) {
//			if (LOG.isWarnEnabled()) {
//				LOG.warn("Could not determine statistics due to an io error: "
//						+ ioex.getMessage());
//			}
//		} catch (Throwable t) {
//			if (LOG.isErrorEnabled()) {
//				LOG.error("Unexpected problem while getting the file statistics: "
//						+ t.getMessage(), t);
//			}
//		}
		
		// no statistics available
		return null;
	}
	
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

		// TODO: allow more splits
		if(minNumSplits > 1) {
			throw new RuntimeException("Only one input split supported for local execution right now");
		}

		return new FileInputSplit[]{new FileInputSplit(0, new Path(this.fileTap.getIdentifier()), 0, -1, new String[]{})};
	}
	
	@Override
	public InputSplitAssigner getInputSplitAssigner(FileInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}
	
	@Override
	public void open(FileInputSplit split) throws IOException {

//		if (this.recordReader instanceof Configurable) {
//			((Configurable) this.recordReader).setConf(jobConf);
//		}

		this.it = fileTap.openForRead(this.fp, null);
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
//			this.fp.increment( StepCounters.Tuples_Read, 1 );
//			this.fp.increment( SliceCounters.Tuples_Read, 1 );
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
		out.writeObject(this.fileTap);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.fileTap = (FileTap)in.readObject();
	}

}
