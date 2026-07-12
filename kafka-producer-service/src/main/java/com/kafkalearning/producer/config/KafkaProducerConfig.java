package com.kafkalearning.producer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Explicit ProducerFactory/KafkaTemplate beans, built from Spring Boot's
 * auto-detected KafkaProperties (bootstrap-servers, key/value serializers
 * from application.yml). Declaring these explicitly — rather than relying
 * purely on Boot's implicit auto-configured KafkaTemplate — keeps the
 * construction visible for learning purposes and gives us a seam to
 * customize producer behavior in Phase 3 (idempotence, acks).
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}