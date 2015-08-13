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

package com.dataArtisans.flinkCascading.runtime.coGroup.regularJoin;

import cascading.tuple.Tuple;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TupleAppendCoGrouper implements CoGroupFunction<Tuple2<Tuple, Tuple[]>, Tuple, Tuple2<Tuple, Tuple[]>> {

	private int tupleListPos;
	private int[] keyPos;
	private Tuple2<Tuple, Tuple[]> outT;
	private List<Tuple> buffer;

	public TupleAppendCoGrouper(int tupleListPos, int tupleListSize, int[] keyPos) {
		this.tupleListPos = tupleListPos;
		outT = new Tuple2<Tuple, Tuple[]>(null, new Tuple[tupleListSize]);
		this.keyPos = keyPos;
		this.buffer = new ArrayList<Tuple>();
	}

	@Override
	public void coGroup(Iterable<Tuple2<Tuple, Tuple[]>> left, Iterable<Tuple> right, Collector<Tuple2<Tuple, Tuple[]>> out) throws Exception {

		Iterator<Tuple2<Tuple,Tuple[]>> leftIt = left.iterator();
		Iterator<Tuple> rightIt = right.iterator();

		if(!leftIt.hasNext()) {
			while(rightIt.hasNext()) {
				Tuple t = rightIt.next();
				outT.f0 = t.get(keyPos);
				outT.f1[tupleListPos] = t;
				out.collect(outT);
			}
		}
		else if(!rightIt.hasNext()) {
			while(leftIt.hasNext()) {
				Tuple2<Tuple, Tuple[]> t = leftIt.next();
				t.f1[tupleListPos] = null;
				out.collect(t);
			}
		}
		else {
			// cross both iterator
			while(rightIt.hasNext()) {
				buffer.add(rightIt.next());
			}
			while(leftIt.hasNext()) {
				Tuple2<Tuple, Tuple[]> tLeft = leftIt.next();
				for(Tuple tRight : buffer) {
					tLeft.f1[tupleListPos] = tRight;
					out.collect(tLeft);
				}
			}
			buffer.clear();
		}
	}
}
