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

package com.dataArtisans.flinkCascading.runtime.source;

import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.tap.Tap;

public class SourceStreamGraph extends NodeStreamGraph {

	private TapSourceStage sourceStage;
	private SingleOutBoundaryStage sinkStage;

	public SourceStreamGraph(FlowProcess flowProcess, FlowNode node, Tap tap) {
		super(flowProcess, node);

		sourceStage = translateHead(tap, flowProcess);

		setTraps();
		setScopes();

		printGraph( node.getID(), "source", flowProcess.getCurrentSliceNum() );
		bind();
	}

	private TapSourceStage translateHead(Tap tap, FlowProcess flowProcess) {

		TapSourceStage sourceStage = new TapSourceStage(flowProcess, tap);
		addHead( sourceStage );
		handleDuct( tap, sourceStage );

		return sourceStage;
	}

	public TapSourceStage getSourceStage() {
		return this.sourceStage;
	}

	public SingleOutBoundaryStage getSinkStage() {
		return this.sinkStage;
	}

	@Override
	protected Duct createBoundaryStage( Boundary boundary, IORole role ) {

		if(role == IORole.sink) {
			this.sinkStage = new SingleOutBoundaryStage(this.flowProcess, boundary);
			return this.sinkStage;
		}

		throw new UnsupportedOperationException("Boundary may only be sink in SourceStreamGraph");
	}

	@Override
	protected Gate createCoGroupGate(CoGroup element, IORole role) {
		throw new UnsupportedOperationException("SourceStreamGraph may not have a CoGroupGate");
	}

	@Override
	protected Gate createGroupByGate(GroupBy element, IORole role) {
		throw new UnsupportedOperationException("SourceStreamGraph may not have a GroupByGate");
	}

}
