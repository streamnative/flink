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

package org.apache.flink.connector.pulsar.source.split.strategy.division;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.range.PulsarRange;
import org.apache.flink.connector.pulsar.source.split.strategy.SplitDivisionStrategy;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Range;

import java.util.Collection;
import java.util.Collections;

/** The SplitDivisionStrategy represent no range splitting is required. */
public class NoSplitDivisionStrategy implements SplitDivisionStrategy {
    private static final long serialVersionUID = 6013618337794320928L;

    public static final NoSplitDivisionStrategy INSTANCE = new NoSplitDivisionStrategy();

    private NoSplitDivisionStrategy() {
        // Singleton instance.
    }

    @Override
    public Collection<Range> getRanges(
            String topic,
            PulsarAdmin pulsarAdmin,
            SplitEnumeratorContext<PulsarPartitionSplit> context)
            throws PulsarAdminException {
        return Collections.singletonList(
                Range.of(PulsarRange.FULL_RANGE_START, PulsarRange.FULL_RANGE_END));
    }
}
