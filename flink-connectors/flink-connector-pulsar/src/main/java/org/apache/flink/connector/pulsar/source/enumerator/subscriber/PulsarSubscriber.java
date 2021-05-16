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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.split.strategy.SplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Pulsar consumer allows a few different ways to consume from the topics, including:
 *
 * <ol>
 *   <li>Subscribe from a collection of topics.
 *   <li>Subscribe to a topic pattern by using Java {@code Regex}.
 * </ol>
 *
 * <p>The PulsarSubscriber provides a unified interface for the Pulsar source to support all these
 * two types of subscribing mode.
 */
@PublicEvolving
public interface PulsarSubscriber extends Serializable {

    /**
     * Get the partitions changes compared to the current partition assignment.
     *
     * <p>Although Pulsar partitions can only expand and will not shrink, the partitions may still
     * disappear when the topic is deleted.
     *
     * @param pulsarAdmin The pulsar admin used to retrieve partition information.
     * @param currentAssignment the partitions that are currently assigned to the source readers.
     *
     * @return The partition changes compared with the currently assigned partitions.
     */
    PartitionChange getPartitionChanges(
            PulsarAdmin pulsarAdmin, Set<PartitionRange> currentAssignment)
            throws PulsarAdminException, InterruptedException, IOException;

    /**
     * Get the available partition from the given pulsar admin interface.
     */
    List<PartitionRange> getCurrentPartitions(PulsarAdmin pulsarAdmin)
            throws PulsarAdminException, InterruptedException, IOException;

    void setContext(SplitEnumeratorContext<PulsarPartitionSplit> context);

    /**
     * A container class to hold the newly added partitions and removed partitions.
     */
    class PartitionChange {

        private final Set<PartitionRange> newPartitions;
        private final Set<PartitionRange> removedPartitions;

        public PartitionChange(
                Set<PartitionRange> newPartitions, Set<PartitionRange> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<PartitionRange> getNewTopicRanges() {
            return newPartitions;
        }

        public Set<PartitionRange> getRemovedPartitions() {
            return removedPartitions;
        }

        @Override
        public String toString() {
            return "PartitionChange{" +
                    "newPartitions=" + newPartitions +
                    ", removedPartitions=" + removedPartitions +
                    '}';
        }
    }

    // ----------------- factory methods --------------

    static PulsarSubscriber getTopicListSubscriber(
            SplitDivisionStrategy splitDivisionStrategy, String... topics) {
        return new TopicListSubscriber(splitDivisionStrategy, topics);
    }

    static PulsarSubscriber getTopicPatternSubscriber(
            String namespace,
            SplitDivisionStrategy splitDivisionStrategy,
            Set<String> topicPatterns) {
        return new TopicPatternSubscriber(namespace, splitDivisionStrategy, topicPatterns);
    }
}
