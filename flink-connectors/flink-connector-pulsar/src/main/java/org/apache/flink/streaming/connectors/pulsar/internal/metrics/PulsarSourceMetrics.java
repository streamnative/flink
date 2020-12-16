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

package org.apache.flink.streaming.connectors.pulsar.internal.metrics;

/**
 * A collection of Pulsar consumer metrics related constant strings.
 *
 * <p>The names must not be changed, as that would break backward compatibility for the consumer's metrics.
 */
public class PulsarSourceMetrics {

    public static final String PULSAR_SOURCE_METRICS_GROUP = "PulsarConsumer";

    // ------------------------------------------------------------------------
    //  Per-subtask metrics
    // ------------------------------------------------------------------------

    public static final String COMMITS_SUCCEEDED_METRICS_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRICS_COUNTER = "commitsFailed";

    // ------------------------------------------------------------------------
    //  Per-partition metrics
    // ------------------------------------------------------------------------

    public static final String OFFSETS_BY_TOPIC_METRICS_GROUP = "topic";
    //public static final String OFFSETS_BY_PARTITION_METRICS_GROUP = "partition";

    public static final String CURRENT_OFFSETS_METRICS_GAUGE = "currentOffsets";
    public static final String COMMITTED_OFFSETS_METRICS_GAUGE = "committedOffsets";
}
