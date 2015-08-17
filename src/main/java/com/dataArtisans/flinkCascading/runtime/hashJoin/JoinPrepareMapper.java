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

package com.dataArtisans.flinkCascading.runtime.hashJoin;

import cascading.tuple.Tuple;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class JoinPrepareMapper implements MapPartitionFunction<Tuple, Tuple2<Tuple, Tuple[]>> {

	final private Tuple2<Tuple, Tuple[]> out;
	private final int initialPos;
	final private int[] keyPos;
	final private Tuple empty = new Tuple();

	public JoinPrepareMapper(int numJoinInputs, int[] keyPos) {
		this(numJoinInputs, 0, keyPos);
	}

	public JoinPrepareMapper(int numJoinInputs, int initialPos, int[] keyPos) {
		this.keyPos = keyPos;
		this.initialPos = initialPos;

		Tuple[] tupleList = new Tuple[numJoinInputs];
		for(int i=0; i<numJoinInputs-1; i++) {
			tupleList[i] = new Tuple();
		}
		out = new Tuple2<Tuple, Tuple[]>(null, tupleList);
	}

	@Override
	public void mapPartition(Iterable<Tuple> tuples, Collector<Tuple2<Tuple, Tuple[]>> collector) throws Exception {
		for(Tuple t : tuples) {
			if(keyPos == null) {
				out.f0 = empty;
			}
			else {
				out.f0 = t.get(keyPos);
			}
			out.f1[initialPos] = t;
			collector.collect(out);
		}
	}

}
