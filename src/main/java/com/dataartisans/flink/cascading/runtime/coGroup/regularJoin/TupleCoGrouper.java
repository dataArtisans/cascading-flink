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
import com.dataartisans.flink.cascading.runtime.spilling.SpillListener;
import com.dataartisans.flink.cascading.runtime.spilling.SpillingTupleCollectionFactory;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
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
public class TupleCoGrouper extends RichCoGroupFunction<Tuple, Tuple, Tuple2<Tuple, Tuple[]>> {

	private int tupleListSize;
	private Fields inputFieldsLeft;
	private Fields keyFieldsLeft;
	private Fields inputFieldsRight;
	private Fields keyFieldsRight;

	private transient Tuple rightT;
	private transient Tuple2<Tuple, Tuple[]> outT;
	private transient Collection<Tuple> buffer;

	public TupleCoGrouper(int tupleListSize,
							Fields inputFieldsLeft, Fields keyFieldsLeft,
							Fields inputFieldsRight, Fields keyFieldsRight) {
		this.tupleListSize = tupleListSize;
		this.inputFieldsLeft = inputFieldsLeft;
		this.keyFieldsLeft = keyFieldsLeft;
		this.inputFieldsRight = inputFieldsRight;
		this.keyFieldsRight = keyFieldsRight;
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
			((Spillable) buffer).setSpillListener(new SpillListener(currentProcess, keyFieldsRight, this.getClass()));
		}

		if(inputFieldsRight.size() > 0) {
			this.rightT = Tuple.size(inputFieldsRight.size());
		}
		else {
			this.rightT = Tuple.size(1);
		}
		this.outT = new Tuple2<Tuple, Tuple[]>(null, new Tuple[tupleListSize]);

	}

	@Override
	public void coGroup(Iterable<Tuple> left, Iterable<Tuple> right, Collector<Tuple2<Tuple, Tuple[]>> out) throws Exception {

		Iterator<Tuple> leftIt = left.iterator();
		Iterator<Tuple> rightIt = right.iterator();

		if(!leftIt.hasNext()) {
			// left input is empty
			outT.f1[0] = null;
			// first right
			outT.f1[1] = rightIt.next();
			outT.f0 = outT.f1[1].get(inputFieldsRight, keyFieldsRight);
			out.collect(outT);
			// remaining right
			while(rightIt.hasNext()) {
				outT.f1[1] = rightIt.next();
				out.collect(outT);
			}
		}
		else if(!rightIt.hasNext()) {
			// right input is empty
			outT.f1[1] = null;
			// first left
			outT.f1[0] = leftIt.next();
			outT.f0 = outT.f1[0].get(inputFieldsLeft, keyFieldsLeft);
			out.collect(outT);
			// remaining left
			while(leftIt.hasNext()) {
				outT.f1[0] = leftIt.next();
				out.collect(outT);
			}
		}
		else {
			// both inputs have at least one tuple

			Tuple firstLeftT = leftIt.next();
			Tuple firstRightT = rightIt.next();

			outT.f0 = firstLeftT.get(inputFieldsLeft, keyFieldsLeft);
			outT.f1[0] = firstLeftT;
			outT.f1[1] = firstRightT;
			out.collect(outT);

			if(!rightIt.hasNext()) {
				// right input has only one tuple
				while(leftIt.hasNext()) {
					outT.f1[0] = leftIt.next();
					out.collect(outT);
				}
			}
			else if(!leftIt.hasNext()) {
				// left input has only one input
				while(rightIt.hasNext()) {
					outT.f1[1] = rightIt.next();
					out.collect(outT);
				}
			}
			else {
				// both inputs have more than one input
				// cross both iterators
				buffer.add(firstRightT);
				while(rightIt.hasNext()) {
					outT.f1[1] = rightIt.next();
					out.collect(outT);
					// remember in buffer
					buffer.add(outT.f1[1]);
				}
				while(leftIt.hasNext()) {
					outT.f1[0] = leftIt.next();
					for(Tuple buffered : buffer) {
						if(rightT.size() != buffered.size()) {
							rightT = Tuple.size(buffered.size());
						}
						rightT.setAll(buffered);

						outT.f1[1] = rightT;
						out.collect(outT);
					}
				}
				buffer.clear();
			}
		}
	}
}
