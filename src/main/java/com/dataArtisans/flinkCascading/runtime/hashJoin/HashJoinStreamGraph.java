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

package com.dataArtisans.flinkCascading.runtime.hashJoin;

import cascading.flow.FlowNode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.runtime.boundaryStages.BoundaryOutStage;
import com.dataArtisans.flinkCascading.runtime.util.CollectorOutput;
import com.dataArtisans.flinkCascading.runtime.util.FlinkFlowProcess;
import org.apache.flink.util.Collector;

public class HashJoinStreamGraph extends NodeStreamGraph {

	private JoinBoundaryInStage sourceStage;
	private CollectorOutput sinkStage;

	public HashJoinStreamGraph(FlinkFlowProcess flowProcess, FlowNode node, Boundary source) {

		super(flowProcess, node);

		sourceStage = handleHead(source);

		setTraps();
		setScopes();

		printGraph( node.getID(), "hashjoin", flowProcess.getCurrentSliceNum() );
		bind();
	}

	public void setTupleCollector(Collector<Tuple> tupleCollector) {
		this.sinkStage.setTupleCollector(tupleCollector);
	}

	public JoinBoundaryInStage getSourceStage() {
		return this.sourceStage;
	}

	private JoinBoundaryInStage handleHead( Boundary boundary) {

		JoinBoundaryInStage sourceStage = (JoinBoundaryInStage)createBoundaryStage(boundary, IORole.source);
		addHead( sourceStage );
		handleDuct( boundary, sourceStage );
		return sourceStage;
	}

	@Override
	protected Duct createBoundaryStage( Boundary boundary, IORole role ) {

		if(role == IORole.source) {
			this.sourceStage = new JoinBoundaryInStage(this.flowProcess, boundary );
			return this.sourceStage;
		}
		else if(role == IORole.sink) {
			this.sinkStage = new BoundaryOutStage(this.flowProcess, boundary);
			return (BoundaryOutStage)this.sinkStage;
		}
		else {
			throw new IllegalArgumentException("Boundary must be either source or sink!");
		}
	}

	@Override
	protected Gate createCoGroupGate(CoGroup coGroup, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a CoGroup gate in a HashJoinStreamGraph");
	}

	@Override
	protected Gate createGroupByGate(GroupBy groupBy, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a GroupBy gate in a HashJoinStreamGraph");
	}

	protected Gate createHashJoinGate( HashJoin join ) {
		return new HashJoinGate(this.flowProcess, join);
	}

}
