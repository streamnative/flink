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

import org.apache.flink.shaded.guava18.com.google.common.collect.ComparisonChain;

import org.apache.pulsar.client.api.Range;

import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.util.Objects;

/**
 * This range class is just a wrapper for pulsar client's {@link Range}. The default Range class is
 * not serializable. We have to store it in split, so we create this class and extends the default
 * one.
 *
 * <p>All the properties were immutable, feel free to use this instance anywhere.
 */
public class PulsarRange extends Range implements Serializable, Comparable<PulsarRange> {
    private static final long serialVersionUID = 3176938692775594400L;

    /** The instance for pulsar full range. */
    public static final PulsarRange FULL_RANGE = PulsarRange.ofFullRange();

    /** The start position for full range. */
    public static final int FULL_RANGE_START = 0;

    /** The end position for full range. */
    public static final int FULL_RANGE_END = 65535;

    public PulsarRange() {
        // Add this constructor only for deserialization.
        this(0, 0);
    }

    public PulsarRange(Range range) {
        this(range.getStart(), range.getEnd());
    }

    public PulsarRange(int start, int end) {
        super(start, end);
    }

    public boolean isFullRange() {
        return getStart() == FULL_RANGE_START && getEnd() == FULL_RANGE_END;
    }

    public static PulsarRange of(Range range) {
        return new PulsarRange(range);
    }

    public static PulsarRange of(int start, int end) {
        return new PulsarRange(start, end);
    }

    public static PulsarRange ofFullRange() {
        return new PulsarRange(FULL_RANGE_START, FULL_RANGE_END);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PulsarRange)) {
            return false;
        }
        PulsarRange that = (PulsarRange) o;
        return Objects.equals(getStart(), that.getStart())
                && Objects.equals(getEnd(), that.getEnd());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStart(), getEnd());
    }

    @Override
    public int compareTo(@NotNull PulsarRange o) {
        return ComparisonChain.start()
                .compare(getStart(), o.getStart())
                .compare(getEnd(), o.getEnd())
                .result();
    }
}
