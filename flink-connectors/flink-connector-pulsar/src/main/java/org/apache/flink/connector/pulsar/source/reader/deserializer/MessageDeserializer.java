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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;

/** An interface for the deserialization of Pulsar messages. */
public interface MessageDeserializer<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize a consumer record into the given collector.
     *
     * @param message the {@code Message} to deserialize.
     *
     * @throws IOException if the deserialization failed.
     */
    void deserialize(Message<?> message, Collector<T> collector) throws IOException;

    /**
     * Wraps a Flink {@link DeserializationSchema} to a {@link MessageDeserializer}.
     *
     * @param valueDeserializer the deserializer class used to deserialize the value.
     * @param <V> the value type.
     *
     * @return A {@link MessageDeserializer} that deserialize the value with the given deserializer.
     */
    static <V> MessageDeserializer<V> valueOnly(DeserializationSchema<V> valueDeserializer) {
        return new MessageDeserializer<V>() {
            private static final long serialVersionUID = -765990803584315049L;

            @Override
            public void deserialize(Message<?> message, Collector<V> collector) throws IOException {
                valueDeserializer.deserialize(message.getData(), collector);
            }

            @Override
            public TypeInformation<V> getProducedType() {
                return valueDeserializer.getProducedType();
            }
        };
    }
}
