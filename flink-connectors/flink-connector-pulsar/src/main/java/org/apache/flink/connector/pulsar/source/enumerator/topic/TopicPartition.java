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

package org.apache.flink.connector.pulsar.source.enumerator.topic;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.partitionedTopicName;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Metadata of a partitioned topic. */
public class TopicPartition implements Serializable {
    private static final long serialVersionUID = -1474354741550810953L;

    /**
     * The topic name of the pulsar. It would be a full topic name, if your don't provide the tenant
     * and namespace, we would add them automatically.
     */
    private final String topic;

    /** Index of partition for the topic. It would be zero for non-partitioned topic. */
    private final int partitionId;

    /**
     * If this topic split into different partitions. A topic with zero partitions would be treat as
     * a non-partitioned topic.
     */
    private final boolean partitioned;

    /**
     * The ranges for this topic, used for limiting consume scope. It would be a {@link
     * TopicRange#createFullRange()} full range for some subscription type.
     */
    private final TopicRange range;

    public TopicPartition(String topic, int partitionId, boolean partitioned, TopicRange range) {
        checkNotNull(topic);
        checkNotNull(range);
        checkArgument(partitionId >= 0, "Topic partition id shouldn't below zero.");

        this.topic = topicName(topic);
        this.partitionId = partitionId;
        this.partitioned = partitioned;
        this.range = range;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    /**
     * Pulsar split the topic partition into a bunch of small topics, we would get the real topic
     * name by using this method.
     */
    public String getFullTopicName() {
        if (partitioned) {
            return partitionedTopicName(topic, partitionId);
        } else {
            return topic;
        }
    }

    public TopicRange getRange() {
        return range;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicPartition that = (TopicPartition) o;
        return partitionId == that.partitionId
                && partitioned == that.partitioned
                && topic.equals(that.topic)
                && range.equals(that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partitionId, partitioned, range);
    }

    @Override
    public String toString() {
        return getFullTopicName() + "|" + range;
    }
}
