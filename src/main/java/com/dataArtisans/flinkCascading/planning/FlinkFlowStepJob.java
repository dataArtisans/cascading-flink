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

package com.dataArtisans.flinkCascading.planning;

import cascading.flow.FlowException;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;


public class FlinkFlowStepJob extends FlowStepJob<Configuration>
{
	/** static field to capture errors in hadoop local mode */
	private static Throwable localError;
	/** Field currentConf */
	private final Configuration currentConf;

	private JobSubmissionResult jobSubmission;

	private JobExecutionResult jobResult;
	private Exception jobException;

	private ExecutionEnvironment env;

	public FlinkFlowStepJob( ClientState clientState, FlinkFlowStep flowStep, Configuration currentConf )
	{
		super( clientState, currentConf, flowStep, 1000, 1000 );
		this.currentConf = currentConf;
		this.env = ((FlinkFlowStep)this.flowStep).getExecutionEnvironment();
		this.jobResult = null;
		this.jobException = null;

		if( flowStep.isDebugEnabled() ) {
			flowStep.logDebug("using polling interval: " + pollingInterval);
		}
	}

	@Override
	public Configuration getConfig() {
		return currentConf;
	}

	@Override
	protected FlowStepStats createStepStats(ClientState clientState) {
		return new FlinkFlowStepStats(this.flowStep, clientState);
	}

	protected void internalBlockOnStop() throws IOException {
		// TODO
	}

	protected void internalNonBlockingStart() throws IOException {

		try {

//			TODO
//			Client client = new Client(new Configuration(), Thread.currentThread().getContextClassLoader());
//			FlinkPlan fp = client.getOptimizedPlan(env.createProgramPlan(), env.getParallelism());
//			JobGraph jg = client.getJobGraph(null, fp);
//
//			this.jobSubmission = client.run(jg, false);
//
//			flowStep.logInfo( "submitted Flink job: " + this.jobSubmission.getJobID() );

			this.jobResult = this.env.execute();

		}
		catch (Exception e) {
			jobException = e;
		}
	}

	@Override
	protected void updateNodeStatus( FlowNodeStats flowNodeStats )
	{
		/*
		try
		{
			if( runningJob == null )
				return;

			float progress;

			boolean isMapper = flowNodeStats.getOrdinal() == 0;

			if( isMapper )
				progress = runningJob.mapProgress();
			else
				progress = runningJob.reduceProgress();

			if( progress == 0.0F ) // not yet running, is only started
				return;

			if( progress != 1.0F )
			{
				flowNodeStats.markRunning();
				return;
			}

			if( !flowNodeStats.isRunning() )
				flowNodeStats.markRunning();

			if( isMapper && runningJob.reduceProgress() > 0.0F )
			{
				flowNodeStats.markSuccessful();
				return;
			}

			int jobState = runningJob.getJobState();

			if( JobStatus.SUCCEEDED == jobState )
				flowNodeStats.markSuccessful();
			else if( JobStatus.FAILED == jobState )
				flowNodeStats.markFailed( null ); // todo: find failure
		}
		catch( IOException exception )
		{
			flowStep.logError( "failed setting node status", throwable );
		}
		*/
	}

	@Override
	public boolean isSuccessful() {
		try {
			return super.isSuccessful();
		}
		catch( NullPointerException exception) {
			// TODO
			throw new FlowException( "Hadoop is not keeping a large enough job history, please increase the \'mapred.jobtracker.completeuserjobs.maximum\' property", exception );
		}
	}

	protected boolean internalNonBlockingIsSuccessful() throws IOException {
		return jobResult != null && jobException == null;
	}

	@Override
	protected boolean isRemoteExecution() {
		return false; // TODO
	}

	@Override
	protected Throwable getThrowable() {
		return jobException;
	}

	protected String internalJobId() {
		return jobSubmission.getJobID().toString();
	}

	protected boolean internalNonBlockingIsComplete() throws IOException {
		return jobResult != null || jobException != null;
	}

	protected void dumpDebugInfo() {

		// TODO
		/*
		try
		{
			if( runningJob == null )
				return;

			int jobState = runningJob.getJobState(); // may throw an NPE internally

			flowStep.logWarn( "hadoop job " + runningJob.getID() + " state at " + JobStatus.getJobRunState( jobState ) );
			flowStep.logWarn( "failure info: " + runningJob.getFailureInfo() );

			TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
			flowStep.logWarn( "task completion events identify failed tasks" );
			flowStep.logWarn( "task completion events count: " + events.length );

			for( TaskCompletionEvent event : events )
				flowStep.logWarn( "event = " + event );
		}
		catch( Throwable throwable )
		{
			flowStep.logError( "failed reading task completion events", throwable );
		}
		*/
	}

	protected boolean internalIsStartedRunning() {


		if( this.jobSubmission == null ) {
			return false;
		}
		else {
			return true;
		}

//		try
//		{
//			return runningJob.mapProgress() > 0;
//		}
//		catch( IOException exception )
//		{
//			flowStep.logWarn( "unable to test for map progress", exception );
//			return false;
//		}
	}

	/**
	 * Internal method to report errors that happen on hadoop local mode. Hadoops local
	 * JobRunner does not give access to TaskReports, but we want to be able to capture
	 * the exception and not just print it to stderr. FlowMapper and FlowReducer use this method.
	 *
	 * @param throwable the throwable to be reported.
	 */
	public static void reportLocalError( Throwable throwable )
	{
		localError = throwable;
	}
}
