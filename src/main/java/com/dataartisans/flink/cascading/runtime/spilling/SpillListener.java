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

package com.dataartisans.flink.cascading.runtime.spilling;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.collect.Spillable;
import cascading.tuple.collect.SpillableTupleList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpillListener implements Spillable.SpillListener {

	private static final Logger LOG = LoggerFactory.getLogger(SpillListener.class);

	public enum Spill {
		Num_Spills_Written, Num_Spills_Read, Num_Tuples_Spilled, Duration_Millis_Written
	}

	private final FlowProcess flowProcess;
	private final Fields joinField;
	private final Class splillingClazz;

	public SpillListener( FlowProcess flowProcess, Fields joinField, Class spillingClazz ) {

		this.flowProcess = flowProcess;
		this.joinField = joinField;
		this.splillingClazz = spillingClazz;
	}

	@Override
	public void notifyWriteSpillBegin( Spillable spillable, int spillSize, String spillReason ) {
		int numFiles = spillable.spillCount();

		if( numFiles % 10 == 0 ) {
			LOG.info( "spilling class: {}, spilling group: {}, on grouping: {}, num times: {}, with reason: {}",
					this.splillingClazz.getSimpleName(), new Object[]{joinField.printVerbose(),
							spillable.getGrouping().print(), numFiles + 1, spillReason} );

			Runtime runtime = Runtime.getRuntime();
			long freeMem = runtime.freeMemory() / 1024 / 1024;
			long maxMem = runtime.maxMemory() / 1024 / 1024;
			long totalMem = runtime.totalMemory() / 1024 / 1024;

			LOG.info( "spilling class: {}, mem on spill (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem,
					this.splillingClazz.getSimpleName());
		}

		LOG.info( "spilling class: {}, spilling {} tuples in list to file number {}",
				this.splillingClazz.getSimpleName(), spillSize, numFiles + 1 );

		flowProcess.increment( Spill.Num_Spills_Written, 1 );
		flowProcess.increment( Spill.Num_Tuples_Spilled, spillSize );
	}

	@Override
	public void notifyWriteSpillEnd( SpillableTupleList spillableTupleList, long duration ) {
		flowProcess.increment( Spill.Duration_Millis_Written, duration );
	}

	@Override
	public void notifyReadSpillBegin( Spillable spillable ) {
		flowProcess.increment( Spill.Num_Spills_Read, 1 );
	}
}
