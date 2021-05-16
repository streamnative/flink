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

import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;
import org.apache.flink.connector.pulsar.source.split.strategy.SplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.util.AsyncUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A subscriber to a fixed list of topics.
 */
public class TopicListSubscriber extends AbstractPulsarSubscriber {
    private static final long serialVersionUID = -6917603843104947866L;

    private final SplitDivisionStrategy splitDivisionStrategy;
    private final List<String> topics;

    public TopicListSubscriber(SplitDivisionStrategy splitDivisionStrategy, String... topics) {
        this.splitDivisionStrategy = splitDivisionStrategy;
        checkArgument(topics.length > 0, "At least one topic needs to be specified");
        this.topics = Lists.newArrayList(topics);
    }

    @Override
    public List<PartitionRange> getCurrentPartitions(PulsarAdmin pulsarAdmin)
            throws PulsarAdminException, InterruptedException, IOException {
        List<PartitionRange> partitions = new ArrayList<>();
        try {
            AsyncUtils.parallelAsync(
                    topics,
                    pulsarAdmin.topics()::getPartitionedTopicMetadataAsync,
                    (topic, exception) -> exception.getStatusCode() == 404,
                    (topic, topicMetadata) -> {
                        logger.info("in getCurrentPartitions");
                        int numPartitions = topicMetadata.partitions;
                        // For key-shared mode, one split take over some range for all partitions of
                        // one topic,
                        // if not in key-shared mode, per partition per split for one topic.
                        Collection<Range> ranges =
                                splitDivisionStrategy.getRanges(topic, pulsarAdmin, context);
                        if (numPartitions == 0) {
                            for (Range range : ranges) {
                                partitions.add(new PartitionRange(topic, range));
                            }
                        } else {
                            for (int i = 0; i < numPartitions; i++) {
                                String fullName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i;
                                for (Range range : ranges) {
                                    partitions.add(new PartitionRange(fullName, range));
                                }
                            }
                        }
                    },
                    PulsarAdminException.class);
        } catch (TimeoutException e) {
            throw new IOException("Cannot retrieve topic metadata: " + e.getMessage());
        }

        return partitions;
    }
}
