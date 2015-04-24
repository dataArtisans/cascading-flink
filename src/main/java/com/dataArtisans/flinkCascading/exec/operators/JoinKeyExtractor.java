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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleBuilder;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinKeyExtractor extends RichMapFunction<Tuple, Tuple3<Tuple, Integer, Tuple>> {

	private Fields inputFields;
	private Fields groupingKeys;
	private int inputId;

	private transient TupleBuilder groupKeyBuilder;
	private Tuple3<Tuple, Integer, Tuple> outT;

	public JoinKeyExtractor() {}

	public JoinKeyExtractor(Fields inputFields, Fields groupingKeys, int inputId) {
		this.inputFields = inputFields;
		this.groupingKeys = groupingKeys;
		this.inputId = inputId;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {

		if(!groupingKeys.isNone()) {
			this.groupKeyBuilder = getTupleBuilder(inputFields, groupingKeys);
		}
		else {
			this.groupKeyBuilder = getNullTupleBuilder();
		}

		outT = new Tuple3<Tuple, Integer, Tuple>();
		outT.f1 = inputId;

	}

	@Override
	public Tuple3<Tuple, Integer, Tuple> map(Tuple inT) throws Exception {

		outT.f0 = this.groupKeyBuilder.makeResult(inT, null);
		outT.f2 = inT;

		return outT;
	}


	private TupleBuilder getTupleBuilder(final Fields inputFields, final Fields keyFields) {

		return new TupleBuilder() {

			@Override
			public Tuple makeResult(Tuple input, Tuple output) {
				return input.get( inputFields, keyFields );
			}
		};
	}

	private TupleBuilder getNullTupleBuilder() {
		return new TupleBuilder() {

			@Override
			public Tuple makeResult(Tuple input, Tuple output) {
				return Tuple.NULL;
			}
		};
	}
}
