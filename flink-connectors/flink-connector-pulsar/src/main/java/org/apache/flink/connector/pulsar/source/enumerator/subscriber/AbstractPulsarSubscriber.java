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

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * The base implementations of {@link PulsarSubscriber}.
 */
public abstract class AbstractPulsarSubscriber implements PulsarSubscriber {
    private static final long serialVersionUID = 1721537402108912442L;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected SplitEnumeratorContext<PulsarPartitionSplit> context;

    @Override
    public PartitionChange getPartitionChanges(
            PulsarAdmin pulsarAdmin, Set<PartitionRange> currentAssignment)
            throws PulsarAdminException, InterruptedException, IOException {
        Set<PartitionRange> newPartitions = new HashSet<>();
        Set<PartitionRange> removedPartitions = new HashSet<>(currentAssignment);
        for (PartitionRange partition : getCurrentPartitions(pulsarAdmin)) {
            if (!removedPartitions.remove(partition)) {
                newPartitions.add(partition);
            }
        }
        if (!removedPartitions.isEmpty()) {
            logger.warn(
                    "The following partitions have been removed from the Pulsar cluster. {}",
                    removedPartitions);
        }
        if (!newPartitions.isEmpty()) {
            logger.info(
                    "The following partitions have been added to the Pulsar cluster. {}",
                    newPartitions);
        }

        return new PartitionChange(newPartitions, removedPartitions);
    }

    @Override
    public void setContext(SplitEnumeratorContext<PulsarPartitionSplit> context) {
        this.context = context;
    }
}
