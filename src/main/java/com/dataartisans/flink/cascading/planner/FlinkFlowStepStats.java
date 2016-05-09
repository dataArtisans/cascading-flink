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

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;
import com.dataartisans.flink.cascading.runtime.stats.EnumStringConverter;
import com.dataartisans.flink.cascading.runtime.stats.AccumulatorCache;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FlinkFlowStepStats extends FlowStepStats {

	private AccumulatorCache accumulatorCache;

	protected FlinkFlowStepStats(FlowStep flowStep, ClientState clientState, AccumulatorCache accumulatorCache) {
		super(flowStep, clientState);
		this.accumulatorCache = accumulatorCache;
	}

	@Override
	public void recordChildStats() {
		// TODO
	}

	@Override
	public String getProcessStepID() {
		return null;
	}

	@Override
	public Collection<String> getCounterGroupsMatching(String regex) {
		return null;
	}

	@Override
	public void captureDetail(Type depth) {
		// TODO
	}

	@Override
	public long getLastSuccessfulCounterFetchTime() {
		return accumulatorCache.getLastUpdateTime();
	}

	@Override
	public Collection<String> getCounterGroups() {
		accumulatorCache.update();
		Map<String, Object> currentAccumulators = accumulatorCache.getCurrentAccumulators();
		Set<String> result = new HashSet<String>();

		for (String key : currentAccumulators.keySet()) {
			result.add(EnumStringConverter.groupCounterToGroup(key));
		}
		return result;
	}

	@Override
	public Collection<String> getCountersFor(String group) {
		accumulatorCache.update();
		Map<String, Object> currentAccumulators = accumulatorCache.getCurrentAccumulators();
		Set<String> result = new HashSet<String>();

		for (String key : currentAccumulators.keySet()) {
			if (EnumStringConverter.accInGroup(group, key)) {
				result.add(EnumStringConverter.groupCounterToCounter(key));
			}
		}
		
		return result;
	}

	@Override
	public long getCounterValue(Enum counter) {
		return getCounterValue(EnumStringConverter.enumToGroup(counter), EnumStringConverter.enumToCounter(counter));
	}

	@Override
	public long getCounterValue(String group, String counter) {
		accumulatorCache.update();
		Map<String, Object> currentAccumulators = accumulatorCache.getCurrentAccumulators();

		for (String key : currentAccumulators.keySet()) {
			if (EnumStringConverter.accMatchesGroupCounter(key, group, counter)) {
				Object o = currentAccumulators.get(key);
				return (Long) o;
			}
		}
		// Cascading returns 0 in case of empty accumulators
		return 0;
	}

}
