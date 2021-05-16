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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

import static org.apache.flink.connector.pulsar.source.StopCondition.StopResult.DONT_STOP;
import static org.apache.flink.connector.pulsar.source.StopCondition.StopResult.STOP_AFTER;
import static org.apache.flink.connector.pulsar.source.StopCondition.StopResult.STOP_BEFORE;

/**
 * An interface to control when to stop.
 */
@PublicEvolving
public interface StopCondition extends Serializable {

    StopResult shouldStop(PartitionRange partition, Message<?> message);

    Comparator<MessageId> NON_BATCH_COMPARATOR =
            new Comparator<>() {
                final Comparator<MessageIdImpl> implComparator =
                        Comparator.comparingLong(MessageIdImpl::getLedgerId)
                                .thenComparingLong(MessageIdImpl::getEntryId)
                                .thenComparingInt(MessageIdImpl::getPartitionIndex);

                @Override
                public int compare(MessageId o1, MessageId o2) {
                    return implComparator.compare((MessageIdImpl) o1, (MessageIdImpl) o2);
                }
            };

    default void init(PartitionRange partition, Consumer<byte[]> consumer)
            throws PulsarClientException {
    }

    static StopCondition stopAtMessageId(MessageId id) {
        return (partition, message) -> hitMessageId(message, id) ? STOP_BEFORE : DONT_STOP;
    }

    static boolean hitMessageId(Message<?> message, MessageId id) {
        return NON_BATCH_COMPARATOR.compare(message.getMessageId(), id) >= 0;
    }

    static StopCondition stopAfterMessageId(MessageId id) {
        return (partition, message) -> hitMessageId(message, id) ? STOP_AFTER : DONT_STOP;
    }

    static StopCondition stopAtMessageIds(Map<PartitionRange, MessageId> ids) {
        return (partition, message) ->
                hitMessageId(message, ids.get(partition)) ? STOP_BEFORE : DONT_STOP;
    }

    static StopCondition stopAfterMessageIds(Map<PartitionRange, MessageId> ids) {
        return (partition, message) ->
                hitMessageId(message, ids.get(partition)) ? STOP_AFTER : DONT_STOP;
    }

    static StopCondition stopAtTimestamp(long timestamp) {
        return (partition, message) ->
                message.getEventTime() >= timestamp ? STOP_BEFORE : DONT_STOP;
    }

    static StopCondition stopAfterTimestamp(long timestamp) {
        return (partition, message) -> message.getEventTime() >= timestamp ? STOP_AFTER : DONT_STOP;
    }

    static StopCondition stopAtLast() {
        return new LastStopCondition() {
            private static final long serialVersionUID = -6499436037176292943L;

            @Override
            public StopResult shouldStop(PartitionRange partition, Message<?> message) {
                return lastId == null || hitMessageId(message, lastId) ? STOP_BEFORE : DONT_STOP;
            }
        };
    }

    static StopCondition stopAfterLast() {
        return new LastStopCondition() {
            private static final long serialVersionUID = 5973551258947173755L;

            @Override
            public StopResult shouldStop(PartitionRange partition, Message<?> message) {
                if (lastId == null) {
                    return STOP_BEFORE;
                }
                return hitMessageId(message, lastId) ? STOP_AFTER : DONT_STOP;
            }
        };
    }

    static StopCondition never() {
        return (partition, message) -> DONT_STOP;
    }

    /**
     * Enum for stop condition.
     */
    enum StopResult {
        STOP_BEFORE,
        STOP_AFTER,
        DONT_STOP
    }

    abstract class LastStopCondition implements StopCondition {
        private static final long serialVersionUID = 8822508647807192794L;

        MessageId lastId;

        @Override
        public void init(PartitionRange partition, Consumer<byte[]> consumer)
                throws PulsarClientException {
            if (lastId == null) {
                lastId = consumer.getLastMessageId();
            }
        }
    }
}
