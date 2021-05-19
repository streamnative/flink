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

package org.apache.flink.connector.pulsar.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.enumerator.initializer.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;

import org.apache.pulsar.client.api.MessageId;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link SourceSplit} for a Pulsar partition. */
@Internal
public class PulsarPartitionSplit implements SourceSplit, Serializable, Cloneable {
    private static final long serialVersionUID = -7680060469197244137L;

    private final PartitionRange partition;
    private final StartOffsetInitializer startOffsetInitializer;
    private final StopCondition stopCondition;

    @Nullable private MessageId lastConsumedId;

    public PulsarPartitionSplit(
            PartitionRange partition,
            StartOffsetInitializer startOffsetInitializer,
            StopCondition stopCondition) {
        this.partition = checkNotNull(partition);
        this.startOffsetInitializer = checkNotNull(startOffsetInitializer);
        this.stopCondition = checkNotNull(stopCondition);
    }

    public String getTopic() {
        return partition.getTopic();
    }

    public PartitionRange getPartition() {
        return partition;
    }

    public StartOffsetInitializer getStartOffsetInitializer() {
        return startOffsetInitializer;
    }

    public StopCondition getStopCondition() {
        return stopCondition;
    }

    @Nullable
    public MessageId getLastConsumedId() {
        return lastConsumedId;
    }

    public void setLastConsumedId(MessageId lastConsumedId) {
        this.lastConsumedId = lastConsumedId;
    }

    @Override
    public String splitId() {
        return partition.toString();
    }

    @Override
    public String toString() {
        return String.format("[Partition: %s, lastConsumedId: %s]", partition, lastConsumedId);
    }

    @Override
    public PulsarPartitionSplit clone() {
        try {
            return (PulsarPartitionSplit) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
