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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class TupleOuterJoiner extends RichJoinFunction<Tuple, Tuple, Tuple2<Tuple, Tuple[]>> {

	private int tupleListSize;
	private Fields inputFieldsLeft;
	private Fields keyFieldsLeft;
	private Fields inputFieldsRight;
	private Fields keyFieldsRight;

	private transient Tuple2<Tuple, Tuple[]> outT;

	public TupleOuterJoiner(int tupleListSize,
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
		this.outT = new Tuple2<Tuple, Tuple[]>(null, new Tuple[tupleListSize]);
	}

	@Override
	public Tuple2<Tuple, Tuple[]> join(Tuple leftT, Tuple rightT) throws Exception {

		if(leftT == null) {
			outT.f0 = rightT.get(inputFieldsRight, keyFieldsRight);
		}
		else {
			outT.f0 = leftT.get(inputFieldsLeft, keyFieldsLeft);
		}

		outT.f1[0] = leftT;
		outT.f1[1] = rightT;

		return outT;
	}

}
