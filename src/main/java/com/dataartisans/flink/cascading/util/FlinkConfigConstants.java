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

package com.dataartisans.flink.cascading.util;

public class FlinkConfigConstants {

    public static final String EXECUTION_MODE = "flink.executionMode";
    public static final String EXECUTION_MODE_BATCH = "BATCH";
    public static final String EXECUTION_MODE_PIPELINED = "PIPELINED";

    public static final String PIPELINE_CLASSPATHS = "pipeline.classpaths";

    public static final String LEAK_CLASSLOADER_CHECK = "classloader.check-leaked-classloader";

    public static final String NETWORK_MEMORY_MIN = "taskmanager.memory.network.min";

    public static final String TASKMANAGER_MEMORY_MANAGED_SIZE = "taskmanager.memory.managed.size";

}
