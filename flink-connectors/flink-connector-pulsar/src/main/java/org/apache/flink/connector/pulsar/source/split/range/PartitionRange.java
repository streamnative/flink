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

package org.apache.flink.connector.pulsar.source.split.range;

import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.guava18.com.google.common.collect.ComparisonChain;

import org.apache.pulsar.client.api.Range;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.pulsar.source.split.range.PulsarRange.FULL_RANGE_END;
import static org.apache.flink.connector.pulsar.source.split.range.PulsarRange.FULL_RANGE_START;

/** The range for key_share mode in topic. */
public class PartitionRange implements Serializable, Comparable<PartitionRange> {
    private static final long serialVersionUID = 4327829161560400869L;

    private String topic;

    private PulsarRange range;

    // Add this constructor only for deserialization.
    public PartitionRange() {
        this(null, null);
    }

    /** Create a full range with the given topic. */
    public PartitionRange(String topic) {
        this.topic = topic;
        this.range = PulsarRange.of(FULL_RANGE_START, FULL_RANGE_END);
    }

    public PartitionRange(String topic, Range range) {
        this.topic = topic;
        this.range = PulsarRange.of(range);
    }

    public PartitionRange(String topic, int start, int end) {
        this.topic = topic;
        this.range = PulsarRange.of(start, end);
    }

    public String getTopic() {
        return topic;
    }

    public PulsarRange getRange() {
        return range;
    }

    public boolean isFullRange() {
        return range.isFullRange();
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setRange(Range range) {
        this.range = PulsarRange.of(range);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeUTF(topic);
        out.writeObject(range);
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        this.topic = in.readUTF();
        this.range = (PulsarRange) in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionRange)) {
            return false;
        }
        PartitionRange that = (PartitionRange) o;
        return topic.equals(that.topic) && range.equals(that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, range);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).add("range", range).toString();
    }

    @Override
    public int compareTo(@NotNull PartitionRange o) {
        return ComparisonChain.start().compare(topic, o.topic).compare(range, o.range).result();
    }
}
