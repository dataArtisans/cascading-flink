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

import cascading.flow.planner.Scope;
import cascading.operation.Aggregator;
import cascading.operation.ConcreteCall;
import cascading.pipe.Every;
import cascading.pipe.Operator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import cascading.tuple.util.TupleViews;
import com.dataArtisans.flinkCascading.exec.FlinkCollector;
import com.dataArtisans.flinkCascading.exec.FlinkFlowProcess;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import static cascading.tuple.util.TupleViews.createComposite;
import static cascading.tuple.util.TupleViews.createNarrow;
import static cascading.tuple.util.TupleViews.createOverride;

public class EveryReducer extends RichGroupReduceFunction<Tuple3<Tuple,Tuple,Tuple>, Tuple> {

	private Every every;
	private Scope outgoingScope;
	private Scope incomingScope;

	private Aggregator aggregator;
	private TupleEntry argumentsEntry;
	private TupleBuilder argumentsBuilder;
	private TupleBuilder outgoingBuilder;
	private transient ConcreteCall call;
	private FlinkFlowProcess ffp;

	public EveryReducer() {}

	public EveryReducer(Every every, Scope incomingScope, Scope outgoingScope) {
		this.every = every;
		this.incomingScope = incomingScope;
		this.outgoingScope = outgoingScope;
	}

	@Override
	public void open(Configuration config) {

		this.ffp = new FlinkFlowProcess(this.getRuntimeContext());
		this.aggregator = this.every.getAggregator();

		call = new ConcreteCall( outgoingScope.getArgumentsDeclarator(), outgoingScope.getOperationDeclaredFields() );

		Fields argumentsSelector = outgoingScope.getArgumentsSelector();
		Fields remainderFields = outgoingScope.getRemainderPassThroughFields();
		Fields outgoingSelector = outgoingScope.getOutValuesSelector();

		argumentsEntry = new TupleEntry( outgoingScope.getArgumentsDeclarator(), true );
		argumentsBuilder = createArgumentsBuilder(incomingScope.getIncomingFunctionArgumentFields(), argumentsSelector);
		outgoingBuilder = createOutgoingBuilder( every, incomingScope.getIncomingFunctionPassThroughFields(), argumentsSelector,
				remainderFields, outgoingScope.getOperationDeclaredFields(), outgoingSelector );

		call.setArguments( argumentsEntry );

		aggregator.prepare(ffp, call);

	}

	@Override
	public void reduce(Iterable<Tuple3<Tuple, Tuple, Tuple>> vals, Collector<Tuple> collector) throws Exception {

		// TODO: add support for multiple aggregators!


//		FlinkCollector wrappedCollector = new FlinkCollector(collector, this.outgoingBuilder, outgoingScope.getOperationDeclaredFields() );
//		// TODO: add completeGroup call to Collector!
//		call.setOutputCollector(wrappedCollector);

		// this.function.prepare(ffp, call); // TODO: do we need to prepare the aggregator?

		boolean first = true;
		Tuple key = null;
		Tuple val = null;

		for(Tuple3<Tuple, Tuple, Tuple> v : vals) {

			key = v.f0;
			val = v.f2;

			if(first) {
				// start group

				call.setGroup( new TupleEntry(key) ); // set group key
				call.setArguments( null );  // zero it out
				call.setOutputCollector( null ); // zero it out

				// start group
				aggregator.start(ffp, call);

				first = false;
			}

			argumentsEntry.setTuple( argumentsBuilder.makeResult( val, null ) );
			call.setArguments( argumentsEntry );
			aggregator.aggregate(ffp, call);

		}

		// finish group
		FlinkCollector wrappedCollector = new FlinkCollector(collector, this.outgoingBuilder, outgoingScope.getOperationDeclaredFields() );
		wrappedCollector.setInTuple(key);

		call.setArguments( null );
		call.setOutputCollector( wrappedCollector );

		aggregator.complete(ffp, call);

	}


	protected TupleBuilder createArgumentsBuilder( final Fields incomingFields, final Fields argumentsSelector )
	{
		if( incomingFields.isUnknown() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return input.get(incomingFields, argumentsSelector);
				}
			};
		}

		if( argumentsSelector.isAll() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return input;
				}
			};
		}

		if( argumentsSelector.isNone() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return Tuple.NULL;
				}
			};
		}

		final Fields inputDeclarationFields = Fields.asDeclaration( incomingFields );

		return new TupleBuilder()
		{
			Tuple result = createNarrow( inputDeclarationFields.getPos( argumentsSelector ) );

			@Override
			public Tuple makeResult( Tuple input, Tuple output )
			{
				return TupleViews.reset(result, input);
			}
		};
	}

	protected TupleBuilder createOutgoingBuilder( final Operator operator, final Fields incomingFields, final Fields argumentSelector,
													final Fields remainderFields, final Fields declaredFields, final Fields outgoingSelector ) {
		final Fields inputDeclarationFields = Fields.asDeclaration(incomingFields);

		if (operator.getOutputSelector().isResults()) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return output;
				}
			};
		}

		if (operator.getOutputSelector().isAll() && !(incomingFields.isUnknown() || declaredFields.isUnknown())) {
			return new TupleBuilder() {
				Tuple result = createComposite(inputDeclarationFields, declaredFields);

				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return TupleViews.reset(result, input, output);
				}
			};
		}

		if (operator.getOutputSelector().isReplace()) {
			if (incomingFields.isUnknown()) {
				return new TupleBuilder() {
					Fields resultFields = operator.getFieldDeclaration().isArguments() ? argumentSelector : declaredFields;

					@Override
					public Tuple makeResult(Tuple input, Tuple output) {
						Tuple result = new Tuple(input);

						result.set(Fields.UNKNOWN, resultFields, output);

						return result;
					}
				};
			}

			return new TupleBuilder() {
				Fields resultFields = operator.getFieldDeclaration().isArguments() ? argumentSelector : declaredFields;
				Tuple result = createOverride(inputDeclarationFields, resultFields);

				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return TupleViews.reset(result, input, output);
				}
			};
		}

		if (operator.getOutputSelector().isSwap()) {
			if (remainderFields.size() == 0) { // the same as Fields.RESULTS
				return new TupleBuilder() {
					@Override
					public Tuple makeResult(Tuple input, Tuple output) {
						return output;
					}
				};
			}
			else if (declaredFields.isUnknown()) {
				return new TupleBuilder() {
					@Override
					public Tuple makeResult(Tuple input, Tuple output) {
						return input.get(incomingFields, remainderFields).append(output);
					}
				};
			}
			else {
				return new TupleBuilder() {
					Tuple view = createNarrow(inputDeclarationFields.getPos(remainderFields));
					Tuple result = createComposite(Fields.asDeclaration(remainderFields), declaredFields);

					@Override
					public Tuple makeResult(Tuple input, Tuple output) {
						TupleViews.reset(view, input);

						return TupleViews.reset(result, view, output);
					}
				};
			}
		}

		if (incomingFields.isUnknown() || declaredFields.isUnknown()) {
			return new TupleBuilder() {
				Fields selector = outgoingSelector.isUnknown() ? Fields.ALL : outgoingSelector;
				TupleEntry incoming = new TupleEntry(incomingFields, true);
				TupleEntry declared = new TupleEntry(declaredFields, true);

				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					incoming.setTuple(input);
					declared.setTuple(output);

					return TupleEntry.select(selector, incoming, declared);
				}
			};
		}

		return new TupleBuilder()
		{
			Fields inputFields = operator.getFieldDeclaration().isArguments() ? Fields.mask( inputDeclarationFields, declaredFields ) : inputDeclarationFields;
			Tuple appended = createComposite( inputFields, declaredFields );
			Fields allFields = Fields.resolve( Fields.ALL, inputFields, declaredFields );
			Tuple result = createNarrow( allFields.getPos( outgoingSelector ), appended );


			@Override
			public Tuple makeResult( Tuple input, Tuple output )
			{
				TupleViews.reset( appended, input, output );

				return result;
			}
		};
	}
}
