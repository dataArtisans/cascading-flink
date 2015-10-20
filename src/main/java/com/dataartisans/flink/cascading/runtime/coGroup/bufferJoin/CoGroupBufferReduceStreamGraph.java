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

package com.dataartisans.flink.cascading.runtime.coGroup.bufferJoin;

import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.runtime.boundaryStages.BoundaryOutStage;
import com.dataartisans.flink.cascading.runtime.util.CollectorOutput;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import org.apache.flink.util.Collector;

public class CoGroupBufferReduceStreamGraph extends NodeStreamGraph {

	private CoGroupBufferInGate sourceStage;
	private CollectorOutput sinkStage;

	public CoGroupBufferReduceStreamGraph(FlinkFlowProcess flowProcess, FlowNode node, CoGroup coGroup) {

		super(flowProcess, node);

		buildGraph(coGroup, flowProcess);

		setTraps();
		setScopes();

		printGraph( node.getID(), "cogroup", flowProcess.getCurrentSliceNum() );
		bind();
	}

	public void setTupleCollector(Collector<Tuple> tupleCollector) {
		this.sinkStage.setTupleCollector(tupleCollector);
	}

	public CoGroupBufferInGate getGroupSource() {
		return this.sourceStage;
	}

	private void buildGraph( CoGroup coGroup, FlowProcess flowProcess ) {

		this.sourceStage = new CoGroupBufferInGate(flowProcess, coGroup, IORole.source);
		addHead( sourceStage );
		handleDuct( coGroup, sourceStage );
	}

	@Override
	protected Duct createBoundaryStage( Boundary boundary, IORole role ) {

		if(role == IORole.sink) {
			this.sinkStage = new BoundaryOutStage(this.flowProcess, boundary);
			return (BoundaryOutStage)this.sinkStage;
		}

		throw new IllegalArgumentException("Boundary may only be used as sink in CoGroupBufferReduceStreamGraph");
	}

	@Override
	protected Gate createCoGroupGate(CoGroup coGroup, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a CoGroup gate in a CoGroupBufferReduceStreamGraph");
	}

	@Override
	protected Gate createGroupByGate(GroupBy groupBy, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a GroupBy gate in a CoGroupBufferReduceStreamGraph");
	}

}