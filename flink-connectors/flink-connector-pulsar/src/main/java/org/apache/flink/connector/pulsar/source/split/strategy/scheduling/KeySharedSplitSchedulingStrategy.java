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

package org.apache.flink.connector.pulsar.source.split.strategy.scheduling;

import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.range.PulsarRange;
import org.apache.flink.connector.pulsar.source.split.strategy.SplitSchedulingStrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SplitSchedulingStrategy for keyShared mode. */
public class KeySharedSplitSchedulingStrategy implements SplitSchedulingStrategy {
    private static final long serialVersionUID = 7248957140526275772L;

    public static final KeySharedSplitSchedulingStrategy INSTANCE =
            new KeySharedSplitSchedulingStrategy();

    private final Map<PulsarRange, Integer> rangeToReaders = new HashMap<>();

    private int nextId = 0;

    private KeySharedSplitSchedulingStrategy() {
        // Singleton instance.
    }

    @Override
    public int getIndexOfReader(int numReaders, PulsarPartitionSplit split) {
        PulsarRange pulsarRange = split.getPartition().getRange();
        return rangeToReaders.computeIfAbsent(
                pulsarRange,
                serializableRange -> {
                    rangeToReaders.put(serializableRange, nextId);
                    int readerId = nextId;
                    nextId++;
                    return readerId;
                });
    }

    @Override
    public void addSplitsBack(
            Map<Integer, List<PulsarPartitionSplit>> pendingPartitionSplitAssignment,
            List<PulsarPartitionSplit> splits,
            int subtaskId,
            int numReaders) {
        pendingPartitionSplitAssignment
                .computeIfAbsent(subtaskId, r -> new ArrayList<>())
                .addAll(splits);
    }
}
