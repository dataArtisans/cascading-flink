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

package com.dataartisans.flink.cascading.platform;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.platform.TestPlatform;
import cascading.platform.hadoop.HadoopFailScheme;
import cascading.platform.hadoop.TestLongComparator;
import cascading.platform.hadoop.TestStringComparator;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import com.dataartisans.flink.cascading.FlinkConnector;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import com.dataartisans.flink.cascading.platform.util.FlinkConfigDefScheme;
import data.InputData;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

public class FlinkTestPlatform extends TestPlatform {

	@Override
	public void setUp() throws IOException {
		if(!this.isUseCluster()) {
			this.numMappers = 1;
			this.numGatherPartitions = 1;
		}
		Properties properties = System.getProperties();

		File testDataLocation = new File("./target/data/");
		if (testDataLocation.exists() && testDataLocation.canRead()) {
			properties.setProperty(InputData.TEST_DATA_PATH, testDataLocation.getAbsolutePath());
		} else {
			throw new IllegalStateException("The test data is not available. To extract the data, " +
					"please execute 'mvn clean compile' once.");
		}
	}

	@Override
	public void setNumMapTasks( Map<Object, Object> properties, int numMapTasks ) {
		this.numMappers = numMapTasks;
	}

	@Override
	public void setNumReduceTasks( Map<Object, Object> properties, int numReduceTasks ) {
		this.numReducers = numReduceTasks;
	}

	@Override
	public void setNumGatherPartitionTasks( Map<Object, Object> properties, int numReduceTasks ) {
		this.numGatherPartitions = numReduceTasks;
	}

	@Override
	public Map<Object, Object> getProperties() {
		return new Properties();
	}

	@Override
	public void tearDown() { }

	@Override
	public void copyFromLocal(String s) throws IOException { }

	@Override
	public void copyToLocal(String s) throws IOException { }

	@Override
	public boolean remoteExists(String outputFile) throws IOException {
		return new File(outputFile).exists();
	}

	@Override
	public boolean remoteRemove(String outputFile, boolean recursive) throws IOException {
		if (!remoteExists(outputFile)) {
			return true;
		}

		File file = new File(outputFile);

		if(!recursive || !file.isDirectory()) {
			return file.delete();
		}

		try {
			FileUtils.deleteDirectory(file);
		}
		catch( IOException exception ) {
			return false;
		}

		return !file.exists();
	}

	@Override
	public FlowProcess getFlowProcess() {
		return new FlinkFlowProcess(new Configuration());
	}

	@Override
	public FlowConnector getFlowConnector(Map<Object, Object> properties) {

		properties.put("flink.num.mappers", this.numMappers+"");
		properties.put("flink.num.reducers", this.numGatherPartitions+"");

		return new FlinkConnector(properties);
	}

	@Override
	public Tap getTap(Scheme scheme, String filename, SinkMode mode) {
		return new Hfs(scheme, filename, mode);
	}

	@Override
	public Tap getTextFile(Fields sourceFields, Fields sinkFields, String filename, SinkMode mode) {
		if( sourceFields == null ) {
 			return new Hfs(new TextLine(), filename, mode);
		}

		return new Hfs( new TextLine( sourceFields, sinkFields ), filename, mode );
	}

	@Override
	public Tap getDelimitedFile(Fields fields, boolean hasHeader, String delimiter, String quote,
								Class[] types, String filename, SinkMode mode) {
		return new Hfs( new TextDelimited( fields, hasHeader, delimiter, quote, types ), filename, mode );
	}

	@Override
	public Tap getDelimitedFile(Fields fields, boolean skipHeader, boolean writeHeader, String delimiter,
								String quote, Class[] types, String filename, SinkMode mode) {
		return new Hfs( new TextDelimited( fields, skipHeader, writeHeader, delimiter, quote, types ), filename, mode );
	}

	@Override
	public Tap getDelimitedFile(String delimiter, String quote, FieldTypeResolver fieldTypeResolver, String filename, SinkMode mode) {
		return new Hfs( new TextDelimited( true, new DelimitedParser( delimiter, quote, fieldTypeResolver ) ), filename, mode );
	}

	@Override
	public Tap getPartitionTap(Tap sink, Partition partition, int openThreshold) {
		return new PartitionTap( (Hfs) sink, partition, openThreshold );
	}

	@Override
	public Scheme getTestConfigDefScheme() {
		return new FlinkConfigDefScheme( new Fields( "line" ));
	}

	@Override
	public Scheme getTestFailScheme() {
		return new HadoopFailScheme( new Fields( "line" ) );
	}

	@Override
	public Comparator getLongComparator(boolean reverseSort) {
		return new TestLongComparator( reverseSort );
	}

	@Override
	public Comparator getStringComparator(boolean reverseSort) {
		return new TestStringComparator( reverseSort );
	}

	@Override
	public String getHiddenTemporaryPath() {
		return null;
	}

	@Override
	public boolean isDAG() {
		return true;
	}
}
