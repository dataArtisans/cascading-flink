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

package com.dataartisans.flink.cascading.runtime.stats;


import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.client.program.Client;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResults;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collections;
import java.util.Map;

public class AccumulatorCache {

	private static final Logger LOG = LoggerFactory.getLogger(AccumulatorCache.class);

	private JobID jobID;

	private ActorGateway localJobManager;
	private Client client;

	private volatile Map<String, Object> currentAccumulators = Collections.emptyMap();

	private FiniteDuration defaultTimeout;

	private final long updateIntervalMillis;
	private long lastUpdateTime;

	public AccumulatorCache(int updateIntervalSecs, FiniteDuration defaultTimeout) {
		this.updateIntervalMillis = updateIntervalSecs * 1000;
		this.defaultTimeout = defaultTimeout;
	}

	public void update() {
		update(false);
	}

	public void update(boolean force) {

		long currentTime = System.currentTimeMillis();
		if (!force && currentTime - lastUpdateTime <= updateIntervalMillis) {
			return;
		}

		if (jobID == null) {
			return;
		}

		if (localJobManager != null) {

			scala.concurrent.Future<Object> response =
					localJobManager.ask(new RequestAccumulatorResults(jobID), defaultTimeout);

			try {

				Object result = Await.result(response, defaultTimeout);

				if (result instanceof AccumulatorResultsFound) {
					Map<String, SerializedValue<Object>> serializedAccumulators =
							((AccumulatorResultsFound) result).result();

					currentAccumulators = AccumulatorHelper.deserializeAccumulators(
							serializedAccumulators, ClassLoader.getSystemClassLoader());

					lastUpdateTime = currentTime;

					LOG.debug("Updated accumulators: {}", currentAccumulators);
				} else {
					LOG.warn("Failed to fetch accumulators for job {}.", jobID);
				}

			} catch (Exception e) {
				LOG.error("Error occurred while fetching accumulators for {}.", jobID, e);
			}


		} else if (client != null) {

			try {
				currentAccumulators = client.getAccumulators(jobID);
				lastUpdateTime = currentTime;

				LOG.debug("Updated accumulators: {}", currentAccumulators);
			} catch (Exception e) {
				LOG.error("Failed to fetch accumulators for job {}.", jobID);
			}

		} else {
			throw new IllegalStateException("The accumulator cache has no valid target.");
		}

	}

	public Map<String, Object> getCurrentAccumulators() {
		return currentAccumulators;
	}

	public void setJobID(JobID jobID) {
		this.jobID = jobID;
	}

	public void setLocalJobManager(ActorGateway localJobManager) {
		this.localJobManager = localJobManager;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}
}
