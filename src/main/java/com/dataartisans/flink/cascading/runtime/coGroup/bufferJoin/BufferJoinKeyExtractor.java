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

package com.dataartisans.flink.cascading.runtime.coGroup.bufferJoin;

import cascading.tuple.Tuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class BufferJoinKeyExtractor implements MapFunction<Tuple, Tuple3<Tuple, Integer, Tuple>> {

	private final int[] keyPos;
	private final Tuple3<Tuple, Integer, Tuple> outT;
	private final Tuple defaultKey = new Tuple(1);

	public BufferJoinKeyExtractor(int inputId, int[] keyPos) {
		this.keyPos = keyPos;
		this.outT = new Tuple3<>(null, inputId, null);
	}

	@Override
	public Tuple3<Tuple, Integer, Tuple> map(Tuple value) throws Exception {

		if(keyPos.length > 0) {
			outT.f0 = value.get(keyPos);
		}
		else {
			outT.f0 = defaultKey;
		}
		outT.f2 = value;
		return outT;
	}
}
