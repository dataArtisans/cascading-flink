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

package com.dataartisans.flink.cascading.planner.rules;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.pipe.Boundary;

public class BoundaryElementFactory implements ElementFactory {

	public static final String BOUNDARY_FACTORY = "cascading.registry.boundary";

	@Override
	public FlowElement create( ElementGraph graph, FlowElement flowElement )
	{
		return new Boundary();
	}
}
