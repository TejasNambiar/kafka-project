package com.kafkalearning.consumer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * A producer client owned by the consumer service, used exclusively by
 * {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer}
 * (see {@code KafkaConsumerConfig}) to republish failed records to the
 * dead letter topic.
 *
 * <p>This is <b>not</b> a general-purpose publishing bean. Before Phase 3,
 * the consumer service had no producer-side dependency at all — it only
 * ever read from Kafka. This class exists solely because the DLT pattern
 * requires the consumer to write to a topic (the DLT) when processing
 * fails; nothing else in this service should inject and use this
 * {@link KafkaTemplate} for arbitrary publishing. If the consumer ever
 * needs to publish for a genuine business reason unrelated to failure
 * recovery, that likely deserves its own, separately-named bean rather
 * than reusing this one.
 *
 * <p>Mirrors the producer service's own {@code KafkaProducerConfig} in
 * structure (explicit {@link ProducerFactory}/{@link KafkaTemplate} beans
 * built from Spring Boot's {@link KafkaProperties}), for the same reason:
 * explicit wiring over implicit autoconfiguration keeps construction
 * visible and gives a seam to customize later if needed.
 */
@Configuration
public class KafkaProducerConfig {

    /**
     * Builds the DLT producer factory from Spring Boot's strongly-typed
     * {@link KafkaProperties} binding, so it automatically inherits the
     * same {@code spring.kafka.producer.*} settings (bootstrap servers,
     * serializers) as the rest of the application, without duplicating
     * them here.
     *
     * @param kafkaProperties Spring Boot's bound Kafka configuration
     * @return a factory capable of producing configured Kafka
     *         {@code Producer} client instances for DLT publishing
     */
    @Bean
    public ProducerFactory<String, String> dltProducerFactory(KafkaProperties kafkaProperties){
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
    }

    /**
     * The template injected into
     * {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer}
     * to actually perform DLT publishes. Not intended to be injected
     * anywhere else in this service.
     *
     * @param dltProducerFactory factory used to construct the underlying client
     * @return a reusable, thread-safe Kafka template scoped to DLT publishing
     */
    @Bean
    public KafkaTemplate<String, String> dltKafkaTemplate(ProducerFactory<String, String> dltProducerFactory){
        return new KafkaTemplate<>(dltProducerFactory);
    }
}
