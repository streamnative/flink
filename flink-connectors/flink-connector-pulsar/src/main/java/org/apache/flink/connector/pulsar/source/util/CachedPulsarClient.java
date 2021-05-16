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

package org.apache.flink.connector.pulsar.source.util;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalListener;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentMap;

/**
 * Enable the sharing of same PulsarClient among tasks in a same process.
 */
public final class CachedPulsarClient {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CachedPulsarClient.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String DEFAULT_CACHE_SIZE = "100";

    private static final String PULSAR_CLIENT_CACHE_SIZE_KEY = "pulsar.client.cache.size";

    private CachedPulsarClient() {
        // No public constructor
    }

    private static int getCacheSize() {
        return Integer.parseInt(System.getProperty(PULSAR_CLIENT_CACHE_SIZE_KEY, DEFAULT_CACHE_SIZE));
    }

    private static final RemovalListener<String, PulsarClientImpl> removalListener = notification -> {
        String config = notification.getKey();
        PulsarClientImpl client = notification.getValue();
        LOG.debug(
                "Evicting pulsar client {} with config {}, due to {}",
                client,
                config,
                notification.getCause());

        close(config, client);
    };

    private static final Cache<String, PulsarClientImpl> clientCache = CacheBuilder.newBuilder()
            .maximumSize(getCacheSize())
            .removalListener(removalListener)
            .build();

    private static PulsarClientImpl createPulsarClient(ClientConfigurationData clientConfig) throws PulsarClientException {
        PulsarClientImpl client;
        try {
            client = new PulsarClientImpl(clientConfig);
            LOG.debug(
                    "Created a new instance of PulsarClientImpl for clientConf = {}",
                    clientConfig);
        } catch (PulsarClientException e) {
            LOG.error("Failed to create PulsarClientImpl for clientConf = {}", clientConfig);
            throw e;
        }
        return client;
    }

    public static synchronized PulsarClientImpl getOrCreate(ClientConfigurationData config) throws PulsarClientException {
        String key = serializeKey(config);
        PulsarClientImpl client = clientCache.getIfPresent(key);

        if (client == null) {
            client = createPulsarClient(config);
            clientCache.put(key, client);
        }

        return client;
    }

    private static void close(String clientConfig, PulsarClientImpl client) {
        if (client != null) {
            try {
                LOG.info("Closing the Pulsar client with config {}", clientConfig);
                client.close();
            } catch (PulsarClientException e) {
                LOG.warn(
                        String.format("Error while closing the Pulsar client %s", clientConfig),
                        e);
            }
        }
    }

    private static String serializeKey(ClientConfigurationData clientConfig) {
        try {
            return mapper.writeValueAsString(clientConfig);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static void close(ClientConfigurationData clientConfig) {
        String key = serializeKey(clientConfig);
        clientCache.invalidate(key);
    }

    @VisibleForTesting
    static void clear() {
        LOG.info("Cleaning up guava cache.");
        clientCache.invalidateAll();
    }

    @VisibleForTesting
    static ConcurrentMap<String, PulsarClientImpl> getAsMap() {
        return clientCache.asMap();
    }
}
