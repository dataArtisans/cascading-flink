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

package com.dataArtisans.flinkCascading.exec.groupBy;

import cascading.flow.FlowProcess;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.util.Iterator;

public class GroupByClosure extends JoinerClosure {

	protected Iterator values;

	public GroupByClosure(FlowProcess flowProcess, Fields[] groupingFields, Fields[] valueFields) {
		super(flowProcess, groupingFields, valueFields);
	}

	public void reset( Iterator<Tuple> values ) {
		this.values = values;
	}

	@Override
	public int size() {
		return 1;
	}

	@Override
	public Iterator<Tuple> getIterator(int pos) {
		if(pos != 0) {
			throw new IllegalArgumentException("Invalid grouping position: " + pos);
		}
		return this.values;
	}

	@Override
	public boolean isEmpty(int pos) {
		return values != null;
	}

	@Override
	public Tuple getGroupTuple(Tuple keysTuple) {
		return keysTuple;
	}
}
