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
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.MemorySpliceGate;
import cascading.pipe.HashJoin;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import cascading.tuple.util.TupleHasher;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class InMemoryHashJoinGate extends MemorySpliceGate {

	private static final Logger LOG = LoggerFactory.getLogger(InMemoryHashJoinGate.class);

	private Collection<Tuple>[] collections;
	private ArrayList<Tuple> streamedCollection;

	public InMemoryHashJoinGate(FlowProcess flowProcess, HashJoin join) {
		super( flowProcess, join );
	}

	@Override
	public void prepare()
	{
		super.prepare();

		streamedCollection = new ArrayList<Tuple>( Arrays.asList(new Tuple()) ); // placeholder in collection
		collections = new Collection[ getNumDeclaredIncomingBranches() ];
		collections[ 0 ] = streamedCollection;

		if( nullsAreNotEqual ) {
			LOG.warn("HashJoin does not fully support key comparators where null values are not treated equal");
		}
	}

	public void buildHashtable(RuntimeContext runtimeContext, String[] inputIds) {

		for(int i=1; i<inputIds.length; i++) {
			String inputId = inputIds[i];

			HashTableBuilder htb = new HashTableBuilder(keyBuilder[i], groupComparators[0], groupHasher);

			keyValues[i] = runtimeContext.getBroadcastVariableWithInitializer(inputId, htb);
			keys.addAll(keyValues[i].keySet());
		}

	}

	@Override
	public void receive( Duct previous, TupleEntry incomingEntry ) {

		// only elements of input 0 are received
		Tuple incomingTuple = incomingEntry.getTuple();
		Tuple keyTuple = keyBuilder[0].makeResult( incomingTuple, null ); // view in incomingTuple

		keyTuple = getDelegatedTuple( keyTuple );

		keys.remove( keyTuple );
		streamedCollection.set( 0, incomingTuple ); // no need to copy, temp setting

		performJoinWith( keyTuple );
	}

	private void performJoinWith( Tuple keyTuple ) {
		// never replace the first array, pos == 0
		for( int i = 1; i < keyValues.length; i++ )
		{
			// if key does not exist, #get will create an empty array list,
			// and store the key, which is not a copy
			if( keyValues[ i ].containsKey( keyTuple ) ) {
				collections[i] = keyValues[i].get(keyTuple);
			}
			else {
				collections[i] = Collections.EMPTY_LIST;
			}
		}

		closure.reset( collections );

		keyEntry.setTuple( keyTuple );
		tupleEntryIterator.reset( splice.getJoiner().getIterator( closure ) );

		next.receive( this, grouping );
	}

	@Override
	public void complete( Duct previous ) {

		collections[ 0 ] = Collections.EMPTY_LIST;

		for( Tuple keyTuple : keys ) {
			performJoinWith(keyTuple);
		}

		keys = createKeySet();
		keyValues = createKeyValuesArray();

		super.complete( previous );
	}

	@Override
	protected Set<Tuple> createKeySet() {
		return new TreeSet<Tuple>( getKeyComparator() );
	}

	@Override
	protected boolean isBlockingStreamed() {
		return true;
	}

	public static class HashTableBuilder implements BroadcastVariableInitializer<Tuple, Map<Tuple, Collection<Tuple>>> {

		TupleBuilder keyBuilder;
		Comparator<Tuple> tupleComparator;
		TupleHasher tupleHasher;

		public HashTableBuilder(TupleBuilder keyBuilder, Comparator<Tuple> tupleComparator, TupleHasher tupleHasher) {
			this.keyBuilder = keyBuilder;
			this.tupleComparator = tupleComparator;
			this.tupleHasher = tupleHasher;
		}

		@Override
		public Map<Tuple, Collection<Tuple>> initializeBroadcastVariable(Iterable<Tuple> data) {

			Map<Tuple, Collection<Tuple>> hashTable = createTupleMap();

			for(Tuple incomingTuple : data) {

				incomingTuple = incomingTuple; // TODO: create copy of tuple
				Tuple keyTuple = keyBuilder.makeResult( incomingTuple, null ); // view in incomingTuple

				if( tupleHasher != null ) {
					keyTuple = new DelegatedTuple(keyTuple);
				}

				hashTable.get(keyTuple).add( incomingTuple ); // always a copy
			}

			return hashTable;
		}

		protected Map<Tuple, Collection<Tuple>> createTupleMap() {

			return new HashMap<Tuple, Collection<Tuple>>() {

				@Override
				public Collection<Tuple> get( Object object ) {

					Collection<Tuple> value = super.get( object );

					if( value == null ) {
						value = new ArrayList<Tuple>();
						super.put( (Tuple) object, value );
					}

					return value;
				}
			};
		}

		protected class DelegatedTuple extends Tuple {
			public DelegatedTuple( Tuple wrapped )
			{
				// pass it in to prevent one being allocated
				super( Tuple.elements( wrapped ) );
			}

			@Override
			public boolean equals( Object object )
			{
				return compareTo( object ) == 0;
			}

			@Override
			public int compareTo( Object other )
			{
				return tupleComparator.compare(this, (Tuple) other);
			}

			@Override
			public int hashCode()
			{
				return tupleHasher.hashCode( this );
			}
		}
	}

}
