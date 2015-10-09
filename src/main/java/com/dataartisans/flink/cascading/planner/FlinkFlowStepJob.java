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

package com.dataartisans.flink.cascading.planner;

import akka.actor.ActorSystem;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import com.dataartisans.flink.cascading.runtime.stats.AccumulatorCache;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
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

	private AccumulatorCache accumulatorCache;

	private Future<JobSubmissionResult> jobSubmission;

	private ExecutorService executorService = Executors.newFixedThreadPool(1);

	private static final int accumulatorUpdateIntervalSecs = 10;

	private volatile static FlinkMiniCluster localCluster;
	private volatile static int localClusterUsers;
	private static final Object lock = new Object();

	private static final FiniteDuration DEFAULT_TIMEOUT = new FiniteDuration(60, TimeUnit.SECONDS);


	public FlinkFlowStepJob( ClientState clientState, FlinkFlowStep flowStep, Configuration currentConf, List<String> classPath ) {

		super(clientState, currentConf, flowStep, 1000, 60000, 60000);

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
		this.accumulatorCache = new AccumulatorCache(accumulatorUpdateIntervalSecs, DEFAULT_TIMEOUT);
		return new FlinkFlowStepStats(this.flowStep, clientState, accumulatorCache);
	}

	protected void internalBlockOnStop() throws IOException {
		if (jobSubmission != null && !jobSubmission.isDone()) {
			if (isLocalExecution()) {
				final ActorGateway jobManager = localCluster.getLeaderGateway(DEFAULT_TIMEOUT);

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
		accumulatorCache.setJobID(jobID);

		Callable<JobSubmissionResult> callable;

		if (isLocalExecution()) {

			flowStep.logInfo("Executing in local mode.");

			final ActorSystem actorSystem = JobClient.startJobClientActorSystem(new org.apache.flink.configuration.Configuration());

			startLocalCluster();

			final ActorGateway jobManager = localCluster.getLeaderGateway(DEFAULT_TIMEOUT);
			accumulatorCache.setLocalJobManager(jobManager);

			JobClient.uploadJarFiles(jobGraph, jobManager, DEFAULT_TIMEOUT);

			callable = new Callable<JobSubmissionResult>() {
				@Override
				public JobSubmissionResult call() throws JobExecutionException {
					return JobClient.submitJobAndWait(actorSystem, jobManager, jobGraph,
							DEFAULT_TIMEOUT, true, ClassLoader.getSystemClassLoader());
				}
			};

		} else {

			flowStep.logInfo("Executing in cluster mode.");

			try {
				String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
				jobGraph.addJar(new Path(path));
				classPath.add(path);
			} catch (URISyntaxException e) {
				throw new IOException("Could not add the submission JAR as a dependency.");
			}

			final Client client = ((ContextEnvironment) env).getClient();
			accumulatorCache.setClient(client);

			final ClassLoader loader;
			List<URL> classPathUrls = new ArrayList<URL>(classPath.size());
			for (String path : classPath) {
				classPathUrls.add(new URL(path));
			}
			loader = JobWithJars.buildUserCodeClassLoader(classPathUrls, Collections.<URL>emptyList(),getClass().getClassLoader());

			callable = new Callable<JobSubmissionResult>() {
				@Override
				public JobSubmissionResult call() throws Exception {
					return client.runBlocking(jobGraph, loader);
				}
			};
		}

		jobSubmission = executorService.submit(callable);

		flowStep.logInfo("submitted Flink job: " + jobID);
	}

	@Override
	protected void updateNodeStatus( FlowNodeStats flowNodeStats ) {
		try {
			if (internalNonBlockingIsComplete() && internalNonBlockingIsSuccessful()) {
				flowNodeStats.markSuccessful();
			} else if(internalIsStartedRunning()) {
				flowNodeStats.isRunning();
			} else {
				flowNodeStats.markFailed(jobException);
			}
		} catch (IOException e) {
			flowStep.logError("Failed to update node status.");
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
			accumulatorCache.update(true);
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
	}

	protected boolean internalIsStartedRunning() {
		return jobSubmission != null;
	}

	private boolean isLocalExecution() {
		return env instanceof LocalEnvironment;
	}

	private void startLocalCluster() {
		synchronized (lock) {
			if (localCluster == null) {
				org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
				configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, env.getParallelism() * 2);
				localCluster = new LocalFlinkMiniCluster(configuration);
				localCluster.start();
			}
			localClusterUsers++;
		}
	}

	private void stopCluster() {
		synchronized (lock) {
			if (localCluster != null) {
				if (--localClusterUsers <= 0) {
					localCluster.shutdown();
					localCluster.awaitTermination();
					localCluster = null;
					localClusterUsers = 0;
					accumulatorCache.setLocalJobManager(null);
				}
			}
			if (executorService != null) {
				executorService.shutdown();
			}
		}
	}

}
