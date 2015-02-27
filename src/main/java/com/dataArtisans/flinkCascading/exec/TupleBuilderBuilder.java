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

package com.dataArtisans.flinkCascading.exec;

import cascading.pipe.Operator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import cascading.tuple.util.TupleBuilder;
import cascading.tuple.util.TupleViews;

import static cascading.tuple.util.TupleViews.createNarrow;
import static cascading.tuple.util.TupleViews.createComposite;
import static cascading.tuple.util.TupleViews.createOverride;
import static cascading.tuple.util.TupleViews.reset;

public class TupleBuilderBuilder {

	public static TupleBuilder createArgumentsBuilder( final Fields incomingFields, final Fields argumentsSelector )
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

	public static TupleBuilder createOutgoingBuilder( final Operator operator, final Fields incomingFields, final Fields argumentSelector,
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

	public static TupleBuilder createNarrowBuilder( final Fields incomingFields, final Fields narrowFields )
	{
		if( narrowFields.isNone() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return Tuple.NULL;
				}
			};
		}

		if( incomingFields.isUnknown() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return input.get(incomingFields, narrowFields);
				}
			};
		}

		if( narrowFields.isAll() ) { // dubious this is ever reached
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return input;
				}
			};
		}

		return createDefaultNarrowBuilder( incomingFields, narrowFields );
	}

	private static TupleBuilder createDefaultNarrowBuilder( final Fields incomingFields, final Fields narrowFields )
	{
		return new TupleBuilder()
		{
			Tuple result = createNarrow( incomingFields.getPos( narrowFields ) );

			@Override
			public Tuple makeResult( Tuple input, Tuple output )
			{
				return reset( result, input );
			}
		};
	}

	public static TupleBuilder createNulledBuilder( final Fields incomingFields, final Fields keyField )
	{
		if( incomingFields.isUnknown() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return Tuples.nulledCopy(incomingFields, input, keyField);
				}
			};
		}

		if( keyField.isNone() ) {
			return new TupleBuilder() {
				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return input;
				}
			};
		}

		if( keyField.isAll() ) {
			return new TupleBuilder() {
				Tuple nullTuple = Tuple.size(incomingFields.size());

				@Override
				public Tuple makeResult(Tuple input, Tuple output) {
					return nullTuple;
				}
			};
		}

		return new TupleBuilder()
		{
			Tuple nullTuple = Tuple.size( keyField.size() );
			Tuple result = createOverride( incomingFields, keyField );

			@Override
			public Tuple makeResult( Tuple baseTuple, Tuple output ) {
				return reset( result, baseTuple, nullTuple );
			}
		};
	}

}
