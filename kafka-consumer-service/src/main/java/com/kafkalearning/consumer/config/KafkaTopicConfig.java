package com.kafkalearning.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicit topic provisioning for the two topics this service owns:
 * {@code orders-events.DLT} (Phase 3) and {@code high-value-orders}
 * (Phase 4).
 *
 * <p>Both are deliberately provisioned here, in the consumer module, not
 * the producer's — they are the consumer's own concerns, not part of the
 * contract the producer publishes against. The producer publishes
 * {@code orders-events} and knows nothing about what the consumer does
 * with failures or derived output; the DLT is the consumer's failure-
 * handling detail, and {@code high-value-orders} is a derived stream
 * produced by this service's own Kafka Streams topology, not something
 * the producer is aware of or responsible for.
 *
 * <p>As with the producer's {@code KafkaTopicConfig}, using {@link NewTopic}
 * beans (rather than {@code auto.create.topics.enable=true} or a manual
 * admin-client script) means Spring's {@code KafkaAdmin} creates these
 * topics idempotently on startup — declarative, colocated with the rest
 * of this service's configuration.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * Partition count matches the source {@code orders-events} topic
     * (3), so the {@code partition % 3} mapping used by
     * {@code DeadLetterPublishingRecoverer} (see {@code KafkaConsumerConfig})
     * always resolves to a valid partition on this topic.
     *
     * @param dltTopicName configured DLT topic name
     * @return the {@link NewTopic} definition for the dead letter topic
     */
    @Bean
    public NewTopic ordersEventsDltTopic(@Value("${app.kafka.topic.orders-events-dlt}") String dltTopicName) {
        return TopicBuilder.name(dltTopicName)
                .partitions(3) // matches source topic partition count
                .replicas(1) // single-broker local dev; never 1 in production
                .build();
    }

    /**
     * Sink topic for {@code HighValueOrderTopology}'s filtered output.
     * Partition count is not required to match the source topic for
     * correctness (Streams repartitions internally where needed), but is
     * kept at 3 here for consistency with the rest of this project's
     * topics at this scale.
     *
     * @param topicName configured high-value-orders topic name
     * @return the {@link NewTopic} definition for the high-value-orders topic
     */
    @Bean
    public NewTopic highValueOrdersTopic(
            @Value("${app.kafka.topic.high-value-orders}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
