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

package com.dataArtisans.flinkCascading.types;

import cascading.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.twitter.chill.ScalaKryoInstantiator;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

public class CascadingTupleSerializer extends TypeSerializer<Tuple> {

	private static final long serialVersionUID = 3L;

	// ------------------------------------------------------------------------

	private final List<ExecutionConfig.Entry<Class<?>, Serializer<?>>> registeredTypesWithSerializers;
	private final List<ExecutionConfig.Entry<Class<?>, Class<? extends Serializer<?>>>> registeredTypesWithSerializerClasses;
	private final List<ExecutionConfig.Entry<Class<?>, Serializer<?>>> defaultSerializers;
	private final List<ExecutionConfig.Entry<Class<?>, Class<? extends Serializer<?>>>> defaultSerializerClasses;
	private final List<Class<?>> registeredTypes;

	// ------------------------------------------------------------------------
	// The fields below are lazily initialized after duplication or deserialization.

	private transient Kryo kryo;
	private transient Tuple copyInstance;

	private transient DataOutputView previousOut;
	private transient DataInputView previousIn;

	private transient Input input;
	private transient Output output;

	// ------------------------------------------------------------------------

	public CascadingTupleSerializer(ExecutionConfig executionConfig){

		this.defaultSerializers = executionConfig.getDefaultKryoSerializers();
		this.defaultSerializerClasses = executionConfig.getDefaultKryoSerializerClasses();
		this.registeredTypesWithSerializers = executionConfig.getRegisteredTypesWithKryoSerializers();
		this.registeredTypesWithSerializerClasses = executionConfig.getRegisteredTypesWithKryoSerializerClasses();
		this.registeredTypes = executionConfig.getRegisteredKryoTypes();
	}

	/**
	 * Copy-constructor that does not copy transient fields. They will be initialized once required.
	 */
	protected CascadingTupleSerializer(CascadingTupleSerializer toCopy) {
		registeredTypesWithSerializers = toCopy.registeredTypesWithSerializers;
		registeredTypesWithSerializerClasses = toCopy.registeredTypesWithSerializerClasses;
		defaultSerializers = toCopy.defaultSerializers;
		defaultSerializerClasses = toCopy.defaultSerializerClasses;
		registeredTypes = toCopy.registeredTypes;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public CascadingTupleSerializer duplicate() {
		return new CascadingTupleSerializer(this);
	}

	@Override
	public Tuple createInstance() {

		checkKryoInitialized();
		try {
			return kryo.newInstance(Tuple.class);
		} catch(Throwable e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Tuple copy(Tuple from) {
		if (from == null) {
			return null;
		}
		checkKryoInitialized();
		try {
			return kryo.copy(from);
		}
		catch(KryoException ke) {
			// kryo was unable to copy it, so we do it through serialization:
			ByteArrayOutputStream baout = new ByteArrayOutputStream();
			Output output = new Output(baout);

			kryo.writeObject(output, from);

			output.close();

			ByteArrayInputStream bain = new ByteArrayInputStream(baout.toByteArray());
			Input input = new Input(bain);

			return (Tuple)kryo.readObject(input, from.getClass());
		}
	}

	@Override
	public Tuple copy(Tuple from, Tuple reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Tuple record, DataOutputView target) throws IOException {
		checkKryoInitialized();
		if (target != previousOut) {
			DataOutputViewStream outputStream = new DataOutputViewStream(target);
			output = new Output(outputStream);
			previousOut = target;
		}

		try {

			int size = record.size();
			kryo.writeObject(output, size);
			for(int i=0; i<size; i++) {
				kryo.writeClassAndObject(output, record.getObject(i));
			}
			output.flush();
		}
		catch (KryoException ke) {
			Throwable cause = ke.getCause();
			if (cause instanceof EOFException) {
				throw (EOFException) cause;
			}
			else {
				throw ke;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Tuple deserialize(DataInputView source) throws IOException {
		checkKryoInitialized();
		if (source != previousIn) {
			DataInputViewStream inputStream = new DataInputViewStream(source);
			input = new NoFetchingInput(inputStream);
			previousIn = source;
		}

		try {

			int size = kryo.readObject(input, Integer.class);
			Tuple outT = new Tuple();
			for(int i=0; i<size; i++) {
				outT.add(kryo.readClassAndObject(input));
			}
			return outT;

		} catch (KryoException ke) {
			Throwable cause = ke.getCause();

			if(cause instanceof EOFException) {
				throw (EOFException) cause;
			} else {
				throw ke;
			}
		}
	}

	@Override
	public Tuple deserialize(Tuple reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		checkKryoInitialized();
		if(this.copyInstance == null){
			this.copyInstance = createInstance();
		}

		Tuple tmp = deserialize(copyInstance, source);
		serialize(tmp, target);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Tuple.class.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof CascadingTupleSerializer) {
			return true;
		} else {
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------

	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new ScalaKryoInstantiator().newKryo();

			// Throwable and all subclasses should be serialized via java serialization
			kryo.addDefaultSerializer(Throwable.class, new JavaSerializer());

			// Add default serializers first, so that they type registrations without a serializer
			// are registered with a default serializer
			for(ExecutionConfig.Entry<Class<?>, Serializer<?>> serializer : defaultSerializers) {
				kryo.addDefaultSerializer(serializer.getKey(), serializer.getValue());
			}
			for(ExecutionConfig.Entry<Class<?>, Class<? extends Serializer<?>>> serializer : defaultSerializerClasses) {
				kryo.addDefaultSerializer(serializer.getKey(), serializer.getValue());
			}

			// register the type of our class
			kryo.register(Tuple.class);

			// register given types. we do this first so that any registration of a
			// more specific serializer overrides this
			for (Class<?> type : registeredTypes) {
				kryo.register(type);
			}

			// register given serializer classes
			for (ExecutionConfig.Entry<Class<?>, Class<? extends Serializer<?>>> e : registeredTypesWithSerializerClasses) {
				Class<?> typeClass = e.getKey();
				Class<? extends Serializer<?>> serializerClass = e.getValue();

				Serializer<?> serializer =
						ReflectionSerializerFactory.makeSerializer(kryo, serializerClass, typeClass);
				kryo.register(typeClass, serializer);
			}

			// register given serializers
			for (ExecutionConfig.Entry<Class<?>, Serializer<?>> e : registeredTypesWithSerializers) {
				kryo.register(e.getKey(), e.getValue());
			}
			// this is needed for Avro but can not be added on demand.
			kryo.register(GenericData.Array.class, new Serializers.SpecificInstanceCollectionSerializerForArrayList());

			kryo.setRegistrationRequired(false);
			kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
		}
	}

}
