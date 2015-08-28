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

package com.dataartisans.flink.cascading.runtime.coGroup.regularJoin;

import cascading.flow.FlowProcess;
import cascading.provider.FactoryLoader;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.collect.Spillable;
import cascading.tuple.collect.TupleCollectionFactory;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import com.dataartisans.flink.cascading.runtime.spilling.SpillListener;
import com.dataartisans.flink.cascading.runtime.spilling.SpillingTupleCollectionFactory;
import com.dataartisans.flink.cascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Iterator;

import static cascading.tuple.collect.TupleCollectionFactory.TUPLE_COLLECTION_FACTORY;

/**
 * Will be replaced by a native outer join hopefully soon.
 */
public class TupleAppendCoGrouper extends RichCoGroupFunction<Tuple2<Tuple, Tuple[]>, Tuple, Tuple2<Tuple, Tuple[]>> {

	private int tupleListPos;
	private int tupleListSize;
	private Fields inputFields;
	private Fields keyFields;
	private int[] keyPos;

	private transient Tuple rightT;
	private transient Tuple2<Tuple, Tuple[]> outT;
	private transient Collection<Tuple> buffer;

	public TupleAppendCoGrouper(int tupleListPos, int tupleListSize, Fields inputFields, Fields keyFields) {
		this.tupleListPos = tupleListPos;
		this.tupleListSize = tupleListSize;
		this.inputFields = inputFields;
		this.keyFields = keyFields;

		this.keyPos = inputFields.getPos(keyFields);
	}

	@Override
	public void open(Configuration config) {

		FlowProcess currentProcess =
				new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config), getRuntimeContext(), "CoGroup");

		FactoryLoader loader = FactoryLoader.getInstance();
		TupleCollectionFactory<org.apache.hadoop.conf.Configuration> tupleCollectionFactory =
				loader.loadFactoryFrom( currentProcess, TUPLE_COLLECTION_FACTORY, SpillingTupleCollectionFactory.class );

		buffer = tupleCollectionFactory.create( currentProcess );

		if( buffer instanceof Spillable) {
			((Spillable) buffer).setSpillListener(new SpillListener(currentProcess, keyFields, this.getClass()));
		}

		if(inputFields.size() > 0) {
			this.rightT = Tuple.size(inputFields.size());
		}
		else {
			this.rightT = Tuple.size(1);
		}
		this.outT = new Tuple2<Tuple, Tuple[]>(null, new Tuple[tupleListSize]);

	}

	@Override
	public void coGroup(Iterable<Tuple2<Tuple, Tuple[]>> left, Iterable<Tuple> right, Collector<Tuple2<Tuple, Tuple[]>> out) throws Exception {

		Iterator<Tuple2<Tuple,Tuple[]>> leftIt = left.iterator();
		Iterator<Tuple> rightIt = right.iterator();

		if(!leftIt.hasNext()) {
			// left input is empty
			while(rightIt.hasNext()) {
				Tuple rightT = rightIt.next();
				outT.f0 = rightT.get(keyPos);
				outT.f1[tupleListPos] = rightT;
				out.collect(outT);
			}
		}
		else if(!rightIt.hasNext()) {
			// right input is empty
			while(leftIt.hasNext()) {
				Tuple2<Tuple, Tuple[]> leftT = leftIt.next();
				leftT.f1[tupleListPos] = null;
				out.collect(leftT);
			}
		}
		else {
			// both inputs have at least one tuple

			Tuple2<Tuple, Tuple[]> firstLeftT = leftIt.next();
			Tuple firstRightT = rightIt.next();

			firstLeftT.f1[tupleListPos] = firstRightT;
			out.collect(firstLeftT);

			if(!rightIt.hasNext()) {
				// right input has only one tuple
				while(leftIt.hasNext()) {
					Tuple2<Tuple, Tuple[]> leftT = leftIt.next();
					leftT.f1[tupleListPos] = firstRightT;
					out.collect(leftT);
				}
			}
			else if(!leftIt.hasNext()) {
				// left input has only one input
				while(rightIt.hasNext()) {
					Tuple rightT = rightIt.next();
					firstLeftT.f1[tupleListPos] = rightT;
					out.collect(firstLeftT);
				}
			}
			else {
				// both inputs have more than one input
				// cross both iterator
				buffer.add(firstRightT);
				while(rightIt.hasNext()) {
					Tuple rightT = rightIt.next();
					firstLeftT.f1[tupleListPos] = rightT;
					out.collect(firstLeftT);
					buffer.add(rightT);
				}
				while(leftIt.hasNext()) {
					Tuple2<Tuple, Tuple[]> streamed = leftIt.next();
					for(Tuple buffered : buffer) {
						if(rightT.size() != buffered.size()) {
							rightT = Tuple.size(buffered.size());
						}
						rightT.setAll(buffered);

						streamed.f1[tupleListPos] = rightT;
						out.collect(streamed);
					}
				}
				buffer.clear();
			}
		}
	}
}
