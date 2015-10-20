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

package com.dataartisans.flink.cascading.runtime.groupBy;

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

public class GroupByStreamGraph extends NodeStreamGraph {

	private GroupByInGate sourceStage;
	private CollectorOutput sinkStage;

	public GroupByStreamGraph(FlinkFlowProcess flowProcess, FlowNode node, GroupBy groupBy) {

		super(flowProcess, node);

		buildGraph(groupBy, flowProcess);

		setTraps();
		setScopes();

		printGraph( node.getID(), "groupby", flowProcess.getCurrentSliceNum() );
		bind();
	}

	public void setTupleCollector(Collector<Tuple> tupleCollector) {
		this.sinkStage.setTupleCollector(tupleCollector);
	}

	public GroupByInGate getGroupSource() {
		return this.sourceStage;
	}

	private void buildGraph( GroupBy groupBy, FlowProcess flowProcess ) {

		this.sourceStage = new GroupByInGate(flowProcess, groupBy, IORole.source);
		addHead( sourceStage );
		handleDuct( groupBy, sourceStage );
	}

	@Override
	protected Duct createBoundaryStage( Boundary boundary, IORole role ) {

		if(role == IORole.sink) {
			this.sinkStage = new BoundaryOutStage(this.flowProcess, boundary);
			return (BoundaryOutStage)this.sinkStage;
		}

		throw new IllegalArgumentException("Boundary may only be used sink in GroupByStreamGraph");
	}

	@Override
	protected Gate createCoGroupGate(CoGroup coGroup, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a CoGroup gate in a GroupByStreamGraph");
	}

	@Override
	protected Gate createGroupByGate(GroupBy groupBy, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a GroupBy gate in a GroupByStreamGraph");
	}

}
