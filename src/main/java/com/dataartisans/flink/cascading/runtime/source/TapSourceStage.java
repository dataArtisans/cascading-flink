/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.runtime.source;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.SourceStage;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TapSourceStage extends SourceStage {

	private static final Logger LOG = LoggerFactory.getLogger(TapSourceStage.class);

	private Tap source;
	private TupleEntryIterator iterator;

	public TapSourceStage(FlowProcess flowProcess, Tap tap) {
		super(flowProcess, tap);

		this.source = tap;
	}

	public void setRecordReader(RecordReader recordReader) throws Throwable {

		try {
			next.start( this );

			// input may be null
			iterator = source.openForRead( flowProcess, recordReader );

		}
		catch( Throwable throwable ) {
			if( !( throwable instanceof OutOfMemoryError ) ) {
				LOG.error("caught throwable", throwable);
			}

			throw throwable;
		}

	}

	public boolean readNextRecord() throws Throwable {

		boolean hasNext = false;

		try {

			while(iterator.hasNext()) {

				// read and forward next record
				TupleEntry tupleEntry;

				try {
					tupleEntry = iterator.next();
					flowProcess.increment(StepCounters.Tuples_Read, 1);
					flowProcess.increment(SliceCounters.Tuples_Read, 1);
				}
				catch (CascadingException exception) {
					handleException(exception, null);
					continue;
				}
				catch (Throwable throwable) {
					handleException(new DuctException("internal error", throwable), null);
					continue;
				}

				next.receive(this, tupleEntry);
				hasNext = true;
				break;
			}
		}
		catch( Throwable throwable ) {

			if( !( throwable instanceof OutOfMemoryError ) ) {
				LOG.error("caught throwable", throwable);
			}

			throw throwable;
		}
		finally {
			try {
				if( iterator != null && !hasNext) {
					iterator.close();
				}
			}
			catch( Throwable currentThrowable ) {
				if( !( currentThrowable instanceof OutOfMemoryError ) ) {
					LOG.warn("failed closing iterator", currentThrowable);
				}

				throw currentThrowable;
			}
		}
		return hasNext;
	}

}
