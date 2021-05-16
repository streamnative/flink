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
import org.apache.flink.connector.pulsar.source.split.strategy.division.NoSplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.split.strategy.division.UniformSplitDivisionStrategy;
import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;
import org.apache.flink.connector.pulsar.source.split.range.PulsarRange;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.pulsar.common.naming.TopicName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PulsarSubscriber}. */
public class PulsarSubscriberTest extends PulsarTestBase {
    private static final String TOPIC1 = TopicName.get("topic1").toString();
    private static final String TOPIC2 = TopicName.get("pattern-topic").toString();
    private static final String TOPIC1_WITH_PARTITION =
            TopicName.get("topic1-partition-0").toString();
    private static final String TOPIC2_WITH_PARTITION =
            TopicName.get("pattern-topic-partition-0").toString();
    private static final PartitionRange assignedPartition1 =
            new PartitionRange(TOPIC1_WITH_PARTITION, PulsarRange.FULL_RANGE);
    private static final PartitionRange assignedPartition2 =
            new PartitionRange(TOPIC2_WITH_PARTITION, PulsarRange.FULL_RANGE);
    private static final PartitionRange removedPartition =
            new PartitionRange("removed", PulsarRange.FULL_RANGE);
    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final Set<PartitionRange> currentAssignment =
            Sets.newHashSet(assignedPartition1, assignedPartition2, removedPartition);

    @BeforeClass
    public static void setup() throws Exception {
        pulsarAdmin = getPulsarAdmin();
        pulsarClient = getPulsarClient();
        createTestTopic(TOPIC1, NUM_PARTITIONS_PER_TOPIC);
        createTestTopic(TOPIC2, NUM_PARTITIONS_PER_TOPIC);
    }

    @AfterClass
    public static void clear() throws Exception {
        pulsarAdmin = getPulsarAdmin();
        pulsarAdmin.topics().deletePartitionedTopic(TOPIC1, true);
        pulsarAdmin.topics().deletePartitionedTopic(TOPIC2, true);
    }

    @Test
    public void testKeySharedTopicListSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                PulsarSubscriber.getTopicListSubscriber(
                        UniformSplitDivisionStrategy.INSTANCE, TOPIC1);
        // 10 subtask 5 partition -> 10 split
        SplitEnumeratorContext context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(10);
        subscriber.setContext(context);
        PulsarSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(pulsarAdmin, currentAssignment);
        assertEquals(change.getNewTopicRanges().size(), 50);
    }

    @Test
    public void testTopicListSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                PulsarSubscriber.getTopicListSubscriber(
                        NoSplitDivisionStrategy.INSTANCE, TOPIC1, TOPIC2);
        PulsarSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(pulsarAdmin, currentAssignment);
        Set<PartitionRange> expectedNewPartitions = new HashSet<>(getPartitionsForTopic(TOPIC1));
        expectedNewPartitions.addAll(getPartitionsForTopic(TOPIC2));
        expectedNewPartitions.remove(assignedPartition1);
        expectedNewPartitions.remove(assignedPartition2);
        assertEquals(expectedNewPartitions, change.getNewTopicRanges());
        assertEquals(Collections.singleton(removedPartition), change.getRemovedPartitions());
    }

    @Test
    public void testTopicPatternSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                PulsarSubscriber.getTopicPatternSubscriber(
                        "public/default",
                        NoSplitDivisionStrategy.INSTANCE,
                        Collections.singleton("pattern.*"));
        PulsarSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(pulsarAdmin, currentAssignment);

        Set<PartitionRange> expectedNewPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            if (!(TOPIC2 + TopicName.PARTITIONED_TOPIC_SUFFIX + i)
                    .equals(assignedPartition2.getTopic())) {
                expectedNewPartitions.add(
                        new PartitionRange(
                                TOPIC2 + "-partition-" + i, PulsarRange.FULL_RANGE));
            }
        }
        Set<PartitionRange> expectedRemovedPartitions =
                new HashSet<>(Arrays.asList(assignedPartition1, removedPartition));

        assertEquals(expectedNewPartitions, change.getNewTopicRanges());
        assertEquals(expectedRemovedPartitions, change.getRemovedPartitions());
    }
}
