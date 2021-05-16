/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.pulsar.source.enumerator.initializer.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.range.PartitionRange;
import org.apache.flink.connector.pulsar.source.split.range.PulsarRange;
import org.apache.flink.connector.pulsar.source.util.PulsarAdminUtils;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.TestLogger;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;

/** Start / stop a Pulsar cluster. */
public abstract class PulsarTestBase extends TestLogger {
    private static final Logger log = LoggerFactory.getLogger(PulsarTestBase.class);

    protected static PulsarContainer pulsarService;

    protected static String serviceUrl;

    protected static String adminUrl;

    protected static String zkUrl;

    protected static Configuration configuration = new Configuration();

    protected static ClientConfigurationData clientConfigurationData =
            new ClientConfigurationData();

    protected static ConsumerConfigurationData<byte[]> consumerConfigurationData =
            new ConsumerConfigurationData<>();

    protected static PulsarAdmin pulsarAdmin;

    protected static PulsarClient pulsarClient;

    protected static List<String> topics = new ArrayList<>();

    public static String getServiceUrl() {
        return serviceUrl;
    }

    public static String getAdminUrl() {
        return adminUrl;
    }

    @BeforeClass
    public static void prepare() throws Exception {
        adminUrl = System.getenv("PULSAR_ADMIN_URL");
        serviceUrl = System.getenv("PULSAR_SERVICE_URL");
        zkUrl = System.getenv("PULSAR_ZK_URL");

        log.info("    Starting PulsarTestBase ");
        if (StringUtils.isNotBlank(adminUrl) && StringUtils.isNotBlank(serviceUrl)) {
            log.info("    Use extend Pulsar Service ");
        } else {
            final String pulsarImage =
                    System.getProperty("pulsar.systemtest.image", "apachepulsar/pulsar:2.7.0");
            pulsarService = new PulsarContainer(DockerImageName.parse(pulsarImage));
            pulsarService.addExposedPort(2181);
            pulsarService.waitingFor(
                    new HttpWaitStrategy()
                            .forPort(BROKER_HTTP_PORT)
                            .forStatusCode(200)
                            .forPath("/admin/v2/namespaces/public/default")
                            .withStartupTimeout(Duration.of(40, SECONDS)));
            pulsarService.start();
            pulsarService.followOutput(new Slf4jLogConsumer(log));
            serviceUrl = pulsarService.getPulsarBrokerUrl();
            adminUrl = pulsarService.getHttpServiceUrl();
            zkUrl = "localhost:" + pulsarService.getMappedPort(2181);
        }
        clientConfigurationData.setServiceUrl(serviceUrl);
        consumerConfigurationData.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfigurationData.setSubscriptionName("flink-" + UUID.randomUUID());

        log.info("Successfully started pulsar service");
    }

    @AfterClass
    public static void shutDownServices() throws Exception {
        log.info("-------------------------------------------------------------------------");
        log.info("    Shut down PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");

        try {
            for (String topic : topics) {
                deleteTestTopic(topic, true);
            }
        } catch (Exception e) {
            log.warn("", e);
        }
        TestStreamEnvironment.unsetAsContext();
        if (pulsarService != null) {
            pulsarService.stop();
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }

        log.info("-------------------------------------------------------------------------");
        log.info("    PulsarTestBase finished");
        log.info("-------------------------------------------------------------------------");
    }

    protected static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();

        flinkConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "16m");
        flinkConfig.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "my_reporter."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                JMXReporter.class.getName());
        return flinkConfig;
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic, SchemaType type, List<T> messages, Optional<Integer> partition)
            throws PulsarClientException {

        return sendTypedMessages(topic, type, messages, partition, null);
    }

    public static <T> Producer<T> getProducer(
            String topic, SchemaType type, Optional<Integer> partition, Class<T> tClass)
            throws PulsarClientException {
        String topicName;
        if (partition.isPresent()) {
            topicName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + partition.get();
        } else {
            topicName = topic;
        }

        Producer producer = null;

        PulsarClient client = PulsarClient.builder().serviceUrl(getServiceUrl()).build();
        switch (type) {
            case BOOLEAN:
                producer = client.newProducer(Schema.BOOL).topic(topicName).create();
                break;
            case BYTES:
                producer = client.newProducer(Schema.BYTES).topic(topicName).create();
                break;
            case LOCAL_DATE:
                producer = client.newProducer(Schema.LOCAL_DATE).topic(topicName).create();
                break;
            case DATE:
                producer = client.newProducer(Schema.DATE).topic(topicName).create();
                break;
            case STRING:
                producer = client.newProducer(Schema.STRING).topic(topicName).create();
                break;
            case TIMESTAMP:
                producer = client.newProducer(Schema.TIMESTAMP).topic(topicName).create();
                break;
            case LOCAL_DATE_TIME:
                producer = client.newProducer(Schema.LOCAL_DATE_TIME).topic(topicName).create();
                break;
            case INT8:
                producer = client.newProducer(Schema.INT8).topic(topicName).create();
                break;
            case DOUBLE:
                producer = client.newProducer(Schema.DOUBLE).topic(topicName).create();
                break;
            case FLOAT:
                producer = client.newProducer(Schema.FLOAT).topic(topicName).create();
                break;
            case INT32:
                producer = client.newProducer(Schema.INT32).topic(topicName).create();
                break;
            case INT16:
                producer = client.newProducer(Schema.INT16).topic(topicName).create();
                break;
            case INT64:
                producer = client.newProducer(Schema.INT64).topic(topicName).create();
                break;
            case AVRO:
                SchemaDefinition<Object> schemaDefinition =
                        SchemaDefinition.builder()
                                .withPojo(tClass)
                                .withJSR310ConversionEnabled(true)
                                .build();
                producer =
                        client.newProducer(Schema.AVRO(schemaDefinition)).topic(topicName).create();
                break;
            case JSON:
                producer = client.newProducer(Schema.JSON(tClass)).topic(topicName).create();
                break;

            default:
                throw new NotImplementedException("Unsupported type " + type);
        }
        return producer;
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition,
            Class<T> tClass)
            throws PulsarClientException {

        Producer<T> producer = getProducer(topic, type, partition, tClass);
        List<MessageId> mids = new ArrayList<>();

        for (T message : messages) {
            MessageId mid = sendMessageInternal(producer, message, null, null, null, null);
            log.info("Sent {} of mid: {}", message.toString(), mid.toString());
            mids.add(mid);
        }

        return mids;
    }

    public static <T> List<MessageId> sendTypedMessagesWithMetadata(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition,
            Class<T> tClass,
            List<Long> eventTimes,
            List<Long> sequenceIds,
            List<Map<String, String>> properties,
            List<String> keys)
            throws PulsarClientException {
        Producer<T> producer = getProducer(topic, type, partition, tClass);
        List<MessageId> mids = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            MessageId mid =
                    sendMessageInternal(
                            producer,
                            messages.get(i),
                            eventTimes.get(i),
                            sequenceIds.get(i),
                            properties.get(i),
                            keys.get(i));
            log.info("Sent {} of mid: {}", messages.get(i).toString(), mid.toString());
            mids.add(mid);
        }
        return mids;
    }

    private static <T> MessageId sendMessageInternal(
            Producer<T> producer,
            T message,
            Long eventTime,
            Long sequenceId,
            Map<String, String> properties,
            String key)
            throws PulsarClientException {
        TypedMessageBuilder<T> mb = producer.newMessage().value(message);
        if (eventTime != null) {
            mb = mb.eventTime(eventTime);
        }
        if (sequenceId != null) {
            mb = mb.sequenceId(sequenceId);
        }
        if (properties != null) {
            mb = mb.properties(properties);
        }
        if (key != null) {
            mb = mb.key(key);
        }
        return mb.send();
    }

    // --------------------- public client related helpers ------------------

    public static PulsarAdmin getPulsarAdmin() {
        try {
            return PulsarAdminUtils.newAdminFromConf(adminUrl, clientConfigurationData);
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar admin", e);
        }
    }

    public static PulsarClient getPulsarClient() {
        try {
            return new ClientBuilderImpl(clientConfigurationData).build();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar client", e);
        }
    }

    // ------------------- topic information helpers -------------------

    public static void createTestTopic(String topic, int numberOfPartitions) throws Exception {
        if (pulsarAdmin == null) {
            pulsarAdmin = getPulsarAdmin();
        }
        if (numberOfPartitions == 0) {
            pulsarAdmin.topics().createNonPartitionedTopic(topic);
        } else {
            pulsarAdmin.topics().createPartitionedTopic(topic, numberOfPartitions);
        }
    }

    public static void deleteTestTopic(String topic, boolean isPartitioned) throws Exception {
        if (pulsarAdmin == null) {
            pulsarAdmin = getPulsarAdmin();
        }
        if (isPartitioned) {
            pulsarAdmin.topics().deletePartitionedTopic(topic);
        } else {
            pulsarAdmin.topics().delete(topic);
        }
    }

    public static List<PartitionRange> getPartitionsForTopic(String topic) throws Exception {
        return pulsarClient.getPartitionsForTopic(topic).get().stream()
                .map(pi -> new PartitionRange(pi, PulsarRange.FULL_RANGE))
                .collect(Collectors.toList());
    }

    public static Map<Integer, Map<String, PulsarPartitionSplit>> getSplitsByOwners(
            final Collection<String> topics, final int numSubtasks) throws Exception {
        final Map<Integer, Map<String, PulsarPartitionSplit>> splitsByOwners = new HashMap<>();
        for (String topic : topics) {
            getPartitionsForTopic(topic)
                    .forEach(
                            partition -> {
                                int ownerReader = Math.abs(partition.hashCode()) % numSubtasks;
                                PulsarPartitionSplit split =
                                        new PulsarPartitionSplit(
                                                partition,
                                                StartOffsetInitializer.earliest(),
                                                StopCondition.stopAfterLast());
                                splitsByOwners
                                        .computeIfAbsent(ownerReader, r -> new HashMap<>())
                                        .put(partition.toString(), split);
                            });
        }
        return splitsByOwners;
    }

    public static String newTopic() {
        final String topic = TopicName.get("topic" + RandomStringUtils.randomNumeric(8)).toString();
        topics.add(topic);
        return topic;
    }

    public static String newTopic(String prefix) {
        final String topic = TopicName.get(prefix + RandomStringUtils.randomNumeric(8)).toString();
        topics.add(topic);
        return topic;
    }
}
