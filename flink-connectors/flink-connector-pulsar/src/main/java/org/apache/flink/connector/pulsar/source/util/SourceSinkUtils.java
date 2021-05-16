/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.util;

import org.apache.flink.connector.pulsar.source.split.range.PulsarRange;

import org.apache.pulsar.client.api.Range;

/**
 * Utilities for source sink options parsing.
 */
public final class SourceSinkUtils {

    private SourceSinkUtils() {
        // No public constructor
    }

    /**
     * Get shard information of the task Fragmentation rules, Can be divided equally: each subtask
     * handles the same range of tasks Not evenly divided: each subtask first processes the tasks in
     * the same range, and the remainder part is added to the tasks starting at index 0 until it is
     * used up.
     *
     * @param countOfSubTasks total subtasks
     * @param indexOfSubTasks current subtask index on subtasks
     *
     * @return task range
     */
    public static Range distributeRange(int countOfSubTasks, int indexOfSubTasks) {
        int countOfKey = PulsarRange.FULL_RANGE_END + 1;
        int part = countOfKey / countOfSubTasks;
        int remainder = countOfKey % countOfSubTasks;

        int subTasksStartKey, subTasksEndKey;
        if (indexOfSubTasks < remainder) {
            part++;
            subTasksStartKey = indexOfSubTasks * part;
            subTasksEndKey = indexOfSubTasks * part + part;
        } else {
            subTasksStartKey = indexOfSubTasks * part + remainder;
            subTasksEndKey = indexOfSubTasks * part + part + remainder;
        }

        subTasksEndKey--;

        return Range.of(subTasksStartKey, subTasksEndKey);
    }
}
