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

package com.dataArtisans.flinkCascading.util;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class FlinkConfigConverter {

	public static Configuration toHadoopConfig(org.apache.flink.configuration.Configuration flinkConfig) {
		if(flinkConfig == null) {
			return null;
		}
		Configuration hadoopConfig = new Configuration();
		for(String key : flinkConfig.keySet()) {
			hadoopConfig.set(key, flinkConfig.getString(key, null));
		}
		return hadoopConfig;
	}

	public static org.apache.flink.configuration.Configuration toFlinkConfig(Configuration hadoopConfig) {
		if(hadoopConfig == null) {
			return null;
		}
		org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();
		for(Map.Entry<String, String> e : hadoopConfig) {
			flinkConfig.setString(e.getKey(), e.getValue());
		}
		return flinkConfig;
	}

}
