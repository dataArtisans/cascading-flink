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

package com.dataartisans.flink.cascading.types.tuple;

import cascading.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class NullMaskSerDeUtils {

	public static void writeNullMask(
			Tuple t, DataOutputView target) throws IOException {

		final int length = t.size();
		int b;
		int bytePos;

		for(int fieldPos = 0; fieldPos < length; ) {
			b = 0x00;
			// set bits in byte
			for(bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				b = b << 1;
				// set bit if field is null
				if(t.getObject(fieldPos) == null) {
					b |= 0x01;
				}
			}
			// shift bits if last byte is not completely filled
			for(; bytePos < 8; bytePos++) {
				b = b << 1;
			}
			// write byte
			target.writeByte(b);
		}
	}

	public static void writeNullMask(
			Object[] array, int length, DataOutputView target) throws IOException {

		int b;
		int bytePos;

		for(int fieldPos = 0; fieldPos < length; ) {
			b = 0x00;
			// set bits in byte
			for(bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				b = b << 1;
				// set bit if field is null
				if(array[fieldPos] == null) {
					b |= 0x01;
				}
			}
			// shift bits if last byte is not completely filled
			for(; bytePos < 8; bytePos++) {
				b = b << 1;
			}
			// write byte
			target.writeByte(b);
		}
	}

	public static void readNullMask(
			boolean[] mask, int length, DataInputView source) throws IOException {

		for(int fieldPos = 0; fieldPos < length; ) {
			// read byte
			int b = source.readUnsignedByte();
			for(int bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				mask[fieldPos] = (b & 0x80) > 0;
				b = b << 1;
			}
		}
	}

	public static void readAndCopyNullMask(
			boolean[] mask, int length, DataInputView source, DataOutputView target) throws IOException {

		for(int fieldPos = 0; fieldPos < length; ) {
			// read byte
			int b = source.readUnsignedByte();
			// copy byte
			target.writeByte(b);
			for(int bytePos = 0; bytePos < 8 && fieldPos < length; bytePos++, fieldPos++) {
				mask[fieldPos] = (b & 0x80) > 0;
				b = b << 1;
			}
		}
	}

}
