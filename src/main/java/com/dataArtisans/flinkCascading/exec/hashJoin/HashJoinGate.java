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

package com.dataArtisans.flinkCascading.exec.hashJoin;

import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.SpliceGate;
import cascading.pipe.Splice;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryChainIterator;
import org.apache.flink.api.java.tuple.Tuple2;

public class HashJoinGate extends SpliceGate<Tuple2<Tuple, Tuple[]>, TupleEntry> {

	private JoinClosure closure;
	private Joiner joiner;

	private TupleEntryChainIterator entryIterator;

	protected HashJoinGate(FlowProcess flowProcess, Splice splice) {
		super(flowProcess, splice);
	}

	@Override
	public void prepare() {

		int numJoinInputs = this.splice.isSelfJoin() ? this.splice.getNumSelfJoins() + 1 : this.splice.getPrevious().length;

		Fields[] keyFields = new Fields[numJoinInputs];
		Fields[] valueFields = new Fields[numJoinInputs];

		Scope outgoingScope = outgoingScopes.get( 0 );

		if(!this.splice.isSelfJoin()) {
			for(int i=0; i < numJoinInputs; i++) {
				Scope incomingScope = incomingScopes.get( i );
				int ordinal = incomingScope.getOrdinal();

				keyFields[ordinal] = outgoingScope.getKeySelectors().get(incomingScope.getName());
				valueFields[ordinal] = incomingScope.getIncomingSpliceFields();
			}
		}
		else {
			Scope incomingScope = incomingScopes.get(0);

			keyFields[0] = outgoingScope.getKeySelectors().get(incomingScope.getName());
			valueFields[0] = incomingScope.getIncomingSpliceFields();

			for (int i = 1; i < numJoinInputs; i++) {
				keyFields[i] = keyFields[0];
				valueFields[i] = valueFields[0];
			}
		}

		this.closure = new JoinClosure(this.flowProcess, keyFields, valueFields);
		this.joiner = this.splice.getJoiner();
		this.entryIterator = new TupleEntryChainIterator(outgoingScope.getOutValuesFields());

	}

	@Override
	public void receive(Duct previous, Tuple2<Tuple, Tuple[]> t) {

		closure.reset(t);

		entryIterator.reset(joiner.getIterator(closure));

		while(entryIterator.hasNext()) {
			next.receive(this, entryIterator.next());
		}

	}


}
