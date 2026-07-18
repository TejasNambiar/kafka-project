package com.kafkalearning.producer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Explicit {@link ProducerFactory} / {@link KafkaTemplate} bean wiring.
 *
 * <p>Spring Boot can autoconfigure a working {@code KafkaTemplate} purely
 * from {@code application.yml} with no config class at all. We declare
 * these beans explicitly instead, for two reasons: it keeps the producer
 * construction path visible rather than implicit, and it gives us a
 * concrete seam to layer in reliability settings later — see the
 * {@code enable.idempotence} / {@code acks=all} properties added in
 * Phase 3, which flow through this exact wiring with no code change here.
 *
 * <p>{@link ProducerFactory} and {@link KafkaTemplate} are kept as
 * separate beans (rather than one collapsed method) to mirror their real
 * relationship: the factory is responsible for constructing underlying
 * Kafka {@code Producer} client instances from configured properties;
 * the template is the thin, thread-safe wrapper the rest of the app
 * actually injects and calls.
 */
@Configuration
public class KafkaProducerConfig {

    /**
     * Builds the producer factory from Spring Boot's strongly-typed
     * {@link KafkaProperties} binding (everything under
     * {@code spring.kafka.producer.*} in application.yml), rather than
     * re-declaring individual properties here — keeps this class in sync
     * with configuration automatically as settings evolve.
     *
     * @param kafkaProperties Spring Boot's bound Kafka configuration
     * @return a factory capable of producing configured Kafka
     *         {@code Producer} client instances
     */
    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
    }

    /**
     * The bean the rest of the application actually depends on to send
     * records. Thread-safe — designed to be a singleton, injected
     * wherever publishing is needed (see {@code OrderEventProducerImpl}).
     *
     * @param producerFactory factory used to construct the underlying client
     * @return a reusable, thread-safe Kafka template
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}