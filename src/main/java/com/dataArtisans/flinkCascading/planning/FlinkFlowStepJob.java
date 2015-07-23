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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import cascading.flow.FlowException;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.hadoop.conf.Configuration;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class FlinkFlowStepJob extends FlowStepJob<Configuration>
{

	private final Configuration currentConf;

	private Future<SerializedJobExecutionResult> jobSubmission;

	private ExecutionEnvironment env;

	private FlinkMiniCluster localCluster;

	private JobID jobID;
	private Throwable jobException;

	public FlinkFlowStepJob( ClientState clientState, FlinkFlowStep flowStep, Configuration currentConf )
	{
		super( clientState, currentConf, flowStep, 1000, 1000 );
		this.currentConf = currentConf;
		this.env = ((FlinkFlowStep)this.flowStep).getExecutionEnvironment();

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
		if (jobSubmission != null && !jobSubmission.isDone()) {
			final ActorRef jobManager;

			if (env.localExecutionIsAllowed()) {
				jobManager = localCluster.getJobManager();
			} else {
				jobManager = null; //TODO cluster
			}

			scala.concurrent.Future<Object> response = Patterns.ask(jobManager, new JobManagerMessages.CancelJob(jobID), new Timeout(10, TimeUnit.SECONDS));

			try {
				Await.result(response, new Timeout(60, TimeUnit.SECONDS).duration());
			} catch (Exception e) {
				throw new IOException("Canceling the job with ID " + jobID + " failed.", e);
			}
		}
	}

	protected void internalNonBlockingStart() throws IOException {

		Plan plan = env.createProgramPlan();

		Optimizer optimizer = new Optimizer(new DataStatistics(), new org.apache.flink.configuration.Configuration());
		OptimizedPlan optimizedPlan = optimizer.compile(plan);

		final JobGraph jobGraph = new JobGraphGenerator().compileJobGraph(optimizedPlan);
		jobID = jobGraph.getJobID();

		final ActorSystem actorSystem = JobClient.startJobClientActorSystem(new org.apache.flink.configuration.Configuration());

		final ActorRef jobManager;

		// read remote / local configuration here
		org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();

		if (env.localExecutionIsAllowed()) {
			configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, env.getParallelism());

			localCluster = new LocalFlinkMiniCluster(configuration);
			jobManager = localCluster.getJobManager();

		} else {
			jobManager = null;
			// TODO cluster
			try {
				//env.execute();
			} catch (Exception e) {
				jobException = e;
			}
		}

		ExecutorService executor = Executors.newFixedThreadPool(1);
		Callable<SerializedJobExecutionResult> callable = new Callable<SerializedJobExecutionResult>() {
			@Override
			public SerializedJobExecutionResult call() throws JobExecutionException {
				return JobClient.submitJobAndWait(actorSystem, jobManager, jobGraph, new FiniteDuration(10, TimeUnit.SECONDS), true);
			}
		};
		jobSubmission = executor.submit(callable);

		flowStep.logInfo("submitted Flink job: " + jobID);
	}

	@Override
	protected void updateNodeStatus( FlowNodeStats flowNodeStats )
	{

		// TODO

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
		try {
			jobSubmission.get(0, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			return false;
		} catch (ExecutionException e) {
			jobException = e.getCause();
		} catch (TimeoutException e) {
			return false;
		}

		boolean status = jobSubmission.isDone() && jobException == null;

		if (localCluster != null && status) {
			localCluster.stop();
			localCluster = null;
		}
		return status;
	}

	@Override
	protected boolean isRemoteExecution() {
		return !env.localExecutionIsAllowed(); // TODO
	}

	@Override
	protected Throwable getThrowable() {
		return jobException;
	}

	protected String internalJobId() {
		return jobID.toString();
	}

	protected boolean internalNonBlockingIsComplete() throws IOException {
		return jobSubmission.isDone();
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
		return jobSubmission != null;
	}

}
