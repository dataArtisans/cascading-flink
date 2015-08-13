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

package com.dataArtisans.flinkCascading.exec.groupBy;

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
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.dataArtisans.flinkCascading.exec.genericDucts.BoundaryOutStage;
import com.dataArtisans.flinkCascading.exec.genericDucts.GroupByOutGate;
import com.dataArtisans.flinkCascading.exec.util.CollectorOutput;
import com.dataArtisans.flinkCascading.exec.util.FlinkFlowProcess;
import org.apache.flink.util.Collector;

public class ReduceStreamGraph extends NodeStreamGraph {

	private GroupByInGate sourceStage;
	private CollectorOutput sinkStage;

	public ReduceStreamGraph(FlinkFlowProcess flowProcess, FlowNode node, GroupBy groupBy) {

		super(flowProcess, node);

		buildGraph(groupBy, flowProcess);

		setTraps();
		setScopes();

		printGraph( node.getID(), "map", flowProcess.getCurrentSliceNum() );
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
	protected Duct createBoundaryStage( Boundary boundary, IORole role )
	{

		if(role == IORole.sink) {
			this.sinkStage = new BoundaryOutStage(this.flowProcess, boundary);
			return (BoundaryOutStage)this.sinkStage;
		}

		throw new InvalidArgumentException("Boundary may only be used sink in ReduceStreamGraph");

	}

	@Override
	protected Gate createCoGroupGate(CoGroup coGroup, IORole ioRole) {
		if(ioRole == IORole.sink) {
			// TODO create map out gate
			throw new UnsupportedOperationException("Cannot create a CoGroup gate in a MapStreamGraph");
		}
		else {
			throw new UnsupportedOperationException("Cannot create a CoGroup gate in a MapStreamGraph");
		}
	}

	@Override
	protected Gate createGroupByGate(GroupBy groupBy, IORole ioRole) {
		if(ioRole == IORole.sink) {
			this.sinkStage = new GroupByOutGate(flowProcess, groupBy, ioRole);
			return (GroupByOutGate)this.sinkStage;
		}
		else {
			throw new UnsupportedOperationException("Cannot create a GroupBy gate in a MapStreamGraph");
		}
	}

}
