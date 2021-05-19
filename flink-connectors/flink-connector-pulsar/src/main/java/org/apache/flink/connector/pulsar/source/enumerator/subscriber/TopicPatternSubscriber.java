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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A subscriber to a topic pattern. */
public class TopicPatternSubscriber extends AbstractPulsarSubscriber {
    private static final long serialVersionUID = -7471048577725467797L;

    private final String namespace;
    private final SplitDivisionStrategy splitDivisionStrategy;
    private final Pattern topicPattern;

    public TopicPatternSubscriber(
            String namespace,
            SplitDivisionStrategy splitDivisionStrategy,
            Set<String> topicPatterns) {
        this.namespace = checkNotNull(namespace);
        this.splitDivisionStrategy = checkNotNull(splitDivisionStrategy);
        checkArgument(!topicPatterns.isEmpty(), "At least one pattern needs to be specified");
        // shorten patterns and compile into one big pattern
        topicPattern =
                Pattern.compile(
                        topicPatterns.stream()
                                .map(topic -> "persistent://" + namespace + "/" + topic)
                                .collect(Collectors.joining("|")));
    }

    @Override
    public List<PartitionRange> getCurrentPartitions(PulsarAdmin pulsarAdmin)
            throws PulsarAdminException, InterruptedException, IOException {
        List<PartitionRange> partitions = new ArrayList<>();
        Topics topics = pulsarAdmin.topics();

        List<String> partitionedTopicList = topics.getPartitionedTopicList(namespace);
        try {
            AsyncUtils.parallelAsync(
                    partitionedTopicList,
                    pulsarAdmin.topics()::getPartitionedTopicMetadataAsync,
                    (topic, topicMetadata) -> {
                        if (topicPattern.matcher(topic).matches()) {
                            int numPartitions = topicMetadata.partitions;
                            Collection<Range> ranges =
                                    splitDivisionStrategy.getRanges(topic, pulsarAdmin, context);
                            for (Range range : ranges) {
                                if (numPartitions == 0) {
                                    partitions.add(new PartitionRange(topic, range));
                                } else {
                                    for (int partitionIndex = 0;
                                            partitionIndex < topicMetadata.partitions;
                                            partitionIndex++) {
                                        String fullName =
                                                topic
                                                        + TopicName.PARTITIONED_TOPIC_SUFFIX
                                                        + partitionIndex;
                                        partitions.add(new PartitionRange(fullName, range));
                                    }
                                }
                            }
                        }
                    },
                    PulsarAdminException.class);
        } catch (TimeoutException e) {
            throw new IOException("Cannot retrieve partition information: " + e.getMessage());
        }

        return partitions;
    }
}
