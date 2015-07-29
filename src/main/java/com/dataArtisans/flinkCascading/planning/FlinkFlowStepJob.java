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

import akka.actor.ActorSystem;
import cascading.flow.FlowException;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.hadoop.conf.Configuration;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
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

	private JobID jobID;
	private Throwable jobException;

	private List<String> classPath;

	private final ExecutionEnvironment env;

	private static FlinkMiniCluster localCluster;

	private Future<Object> jobSubmission;

	private ExecutorService executorService;

	private static final FiniteDuration DEFAULT_TIMEOUT = new FiniteDuration(60, TimeUnit.SECONDS);


	public FlinkFlowStepJob( ClientState clientState, FlinkFlowStep flowStep, Configuration currentConf, List<String> classPath )
	{
		super( clientState, currentConf, flowStep, 1000, 1000 );
		this.currentConf = currentConf;
		this.env = ((FlinkFlowStep)this.flowStep).getExecutionEnvironment();
		this.classPath = classPath;

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
			if (isLocalExecution()) {
				final ActorGateway jobManager = localCluster.getJobManagerGateway();

				scala.concurrent.Future<Object> response = jobManager.ask(new JobManagerMessages.CancelJob(jobID), DEFAULT_TIMEOUT);

				try {
					Await.result(response, DEFAULT_TIMEOUT);
				} catch (Exception e) {
					throw new IOException("Canceling the job with ID " + jobID + " failed.", e);
				}
			} else {
				try {
					((ContextEnvironment) env).getClient().cancel(jobID);
				} catch (Exception e) {
					throw new IOException("An exception occurred while stopping the Flink job:\n" + e.getMessage());
				}
			}
		}

	}

	protected void internalNonBlockingStart() throws IOException {

		Plan plan = env.createProgramPlan();

		Optimizer optimizer = new Optimizer(new DataStatistics(), new org.apache.flink.configuration.Configuration());
		OptimizedPlan optimizedPlan = optimizer.compile(plan);

		final JobGraph jobGraph = new JobGraphGenerator().compileJobGraph(optimizedPlan);
		for (String jarPath : classPath) {
			jobGraph.addJar(new Path(jarPath));
		}

		jobID = jobGraph.getJobID();

		Callable<Object> callable;

		if (isLocalExecution()) {

			flowStep.logInfo("Executing in local mode.");

			final ActorSystem actorSystem = JobClient.startJobClientActorSystem(new org.apache.flink.configuration.Configuration());

			startLocalCluster();
			final ActorGateway jobManager = localCluster.getJobManagerGateway();

			JobClient.uploadJarFiles(jobGraph, jobManager, DEFAULT_TIMEOUT);

			callable = new Callable<Object>() {
				@Override
				public SerializedJobExecutionResult call() throws JobExecutionException {
					return JobClient.submitJobAndWait(actorSystem, jobManager, jobGraph, DEFAULT_TIMEOUT, true);
				}
			};

		} else {

			flowStep.logInfo("Executing in cluster mode.");

			try {
				String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
				jobGraph.addJar(new Path(path));
			} catch (URISyntaxException e) {
				throw new IOException("Could not add the submission JAR as a dependency.");
			}

			final Client client = ((ContextEnvironment) env).getClient();

			callable = new Callable<Object>() {
				@Override
				public JobSubmissionResult call() throws Exception {
					return client.run(jobGraph, true);
				}
			};
		}


		executorService = Executors.newFixedThreadPool(1);
		jobSubmission = executorService.submit(callable);

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
			return false;
		} catch (TimeoutException e) {
			return false;
		}

		boolean isDone = jobSubmission.isDone();
		if (isDone) {
			stopCluster();
		}
		return isDone;
	}

	@Override
	protected boolean isRemoteExecution() {
		return isLocalExecution();
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

	private boolean isLocalExecution() {
		return env instanceof LocalEnvironment;
	}

	private void startLocalCluster() {
		if (localCluster == null) {
			org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
			configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, env.getParallelism() * 2);
//			configuration.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 128);
			localCluster = new LocalFlinkMiniCluster(configuration);
		}
	}

	private void stopCluster() {
		if (localCluster != null) {
			localCluster.shutdown();
			localCluster = null;
		}
		if (executorService != null) {
			executorService.shutdown();
		}
	}

}
