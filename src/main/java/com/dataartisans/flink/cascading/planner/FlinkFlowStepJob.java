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

import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import com.dataartisans.flink.cascading.runtime.stats.AccumulatorCache;
import com.dataartisans.flink.cascading.util.FlinkConfigConstants;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.ProgramAbortException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class FlinkFlowStepJob extends FlowStepJob<Configuration> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkFlowStepJob.class);
    private static final int accumulatorUpdateIntervalSecs = 10;
    private static final Object lock = new Object();
    private static final FiniteDuration DEFAULT_TIMEOUT = new FiniteDuration(60, TimeUnit.SECONDS);
    private volatile static MiniCluster localCluster;
    private volatile static int localClusterUsers;
    private final Configuration currentConf;
    private final List<String> classPath;
    private final ExecutionEnvironment env;
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private JobClient client;
    private JobID jobID;
    private Throwable jobException;
    private AccumulatorCache accumulatorCache;
    private Future<JobSubmissionResult> jobSubmission;


    public FlinkFlowStepJob(ClientState clientState, FlinkFlowStep flowStep, Configuration currentConf, List<String> classPath) {

        super(clientState, currentConf, flowStep, 1000, 60000, 60000);

        this.currentConf = currentConf;
        this.env = ((FlinkFlowStep) this.flowStep).getExecutionEnvironment();
        this.classPath = classPath.stream().map(jarpath -> "file://" + jarpath).collect(Collectors.toList());

        if (flowStep.isDebugEnabled()) {
            flowStep.logDebug("using polling interval: " + pollingInterval);
        }
    }

    @Override
    public Configuration getConfig() {
        return currentConf;
    }

    @Override
    protected FlowStepStats createStepStats(ClientState clientState) {
        this.accumulatorCache = new AccumulatorCache(accumulatorUpdateIntervalSecs);
        return new FlinkFlowStepStats(this.flowStep, clientState, accumulatorCache);
    }

    protected void internalBlockOnStop() throws IOException {
        if (client != null) {
            try {
                client.cancel().get();
            } catch (Exception e) {
                throw new IOException("An exception occurred while stopping the Flink job with ID: " + jobID + ": " + e.getMessage());
            }
        }

    }

    protected void internalNonBlockingStart() throws IOException {

        // set exchange mode, BATCH is default
        String execMode = getConfig().get(FlinkConfigConstants.EXECUTION_MODE);
        if (execMode == null || FlinkConfigConstants.EXECUTION_MODE_BATCH.equals(execMode)) {
            env.getConfig().setExecutionMode(ExecutionMode.BATCH);
        } else if (FlinkConfigConstants.EXECUTION_MODE_PIPELINED.equals(execMode)) {
            env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
        } else {
            LOG.warn("Unknow value for '" + FlinkConfigConstants.EXECUTION_MODE + "' parameter. " +
                    "Only '" + FlinkConfigConstants.EXECUTION_MODE_BATCH + "' " +
                    "or '" + FlinkConfigConstants.EXECUTION_MODE_PIPELINED + "' supported. " +
                    "Using " + FlinkConfigConstants.EXECUTION_MODE_BATCH + " exchange by default.");
            env.getConfig().setExecutionMode(ExecutionMode.BATCH);
        }

        env.getConfiguration().setString(FlinkConfigConstants.PIPELINE_CLASSPATHS, String.join(",", classPath));

        if (!env.getConfiguration().containsKey(FlinkConfigConstants.LEAK_CLASSLOADER_CHECK)) {
            LOG.warn("disable classloader leak check which failed PlatformTests");
            env.getConfiguration().setBoolean(FlinkConfigConstants.LEAK_CLASSLOADER_CHECK, false);
        }

        if (!env.getConfiguration().containsKey(FlinkConfigConstants.NETWORK_MEMORY_MIN)) {
            env.getConfiguration().setString(FlinkConfigConstants.NETWORK_MEMORY_MIN, "128mb");
        }

        if (!env.getConfiguration().containsKey(FlinkConfigConstants.TASKMANAGER_MEMORY_MANAGED_SIZE)) {
            env.getConfiguration().setString(FlinkConfigConstants.TASKMANAGER_MEMORY_MANAGED_SIZE, "512mb");
        }

        if (isLocalExecution()) {

            flowStep.logInfo("Executing in local mode.");

            try {
                startLocalCluster();
            } catch (Exception e) {
                flowStep.logError("Fail to start local cluster.");
                throw new RuntimeException(e);
            }
        } else if (isRemoteExecution()) {
            flowStep.logInfo("Executing in cluster mode.");
        }

        try {
            client = env.executeAsync();
            jobID = client.getJobID();
            accumulatorCache.setClient(client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        flowStep.logInfo("submitted Flink job: " + jobID);
    }

    @Override
    protected void updateNodeStatus(FlowNodeStats flowNodeStats) {
        try {
            if (internalNonBlockingIsComplete() && internalNonBlockingIsSuccessful()) {
                flowNodeStats.markSuccessful();
            } else if (internalIsStartedRunning()) {
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
            client.getJobExecutionResult().get(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        } catch (ExecutionException e) {
            jobException = e.getCause();
            return false;
        } catch (TimeoutException e) {
            return false;
        }

        boolean isDone = client.getJobExecutionResult().isDone();
        if (isDone) {
            accumulatorCache.update(true);
            accumulatorCache.setClient(null);
            try {
                stopCluster();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return isDone;
    }

    @Override
    public Throwable call() {
        if (env instanceof OptimizerPlanEnvironment) {
            // We have an OptimizerPlanEnvironment.
            //   This environment is only used to to fetch the Flink execution plan.
            try {
                // OptimizerPlanEnvironment does not execute but only build the execution plan.
                env.execute("plan generation");
            }
            // execute() throws a ProgramAbortException if everything goes well
            catch (ProgramAbortException pae) {
                // Forward call() to get Cascading's internal job stats right.
                //   The job will be skipped due to the overridden isSkipFlowStep method.
                super.call();
                // forward expected ProgramAbortException
                return pae;
            }
            //
            catch (Exception e) {
                // forward unexpected exception
                return e;
            }
        }
        // forward to call() if we have a regular ExecutionEnvironment
        return super.call();

    }

    protected boolean isSkipFlowStep() throws IOException {
        if (env instanceof OptimizerPlanEnvironment) {
            // We have an OptimizerPlanEnvironment.
            //   This environment is only used to to fetch the Flink execution plan.
            //   We do not want to execute the job in this case.
            return true;
        } else {
            return super.isSkipFlowStep();
        }
    }

    @Override
    protected boolean isRemoteExecution() {
        return env instanceof ContextEnvironment;
    }

    @Override
    protected Throwable getThrowable() {
        return jobException;
    }

    protected String internalJobId() {
        return jobID.toString();
    }

    protected boolean internalNonBlockingIsComplete() throws IOException {
        try {
            return client.getJobExecutionResult().isDone() || client.getJobExecutionResult().isCompletedExceptionally();
        } catch (Exception e) {
            return false;
        }
    }

    protected void dumpDebugInfo() {
    }

    protected boolean internalIsStartedRunning() {
        try {
            return client.getJobExecutionResult() != null;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isLocalExecution() {
        return env instanceof LocalEnvironment;
    }

    private void startLocalCluster() throws Exception {
        synchronized (lock) {
            if (localCluster == null) {
                final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
                        .setNumSlotsPerTaskManager(env.getParallelism() * 2)
                        .setRpcServiceSharing(RpcServiceSharing.SHARED)
                        .withRandomPorts()
                        .build();
                localCluster = new MiniCluster(miniClusterConfiguration);
                localCluster.start();
            }
            localClusterUsers++;
        }
    }

    private void stopCluster() throws Exception {
        synchronized (lock) {
            if (localCluster != null) {
                if (--localClusterUsers <= 0) {
                    localCluster.close();
                    localCluster = null;
                    localClusterUsers = 0;
                }
            }
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }

}
