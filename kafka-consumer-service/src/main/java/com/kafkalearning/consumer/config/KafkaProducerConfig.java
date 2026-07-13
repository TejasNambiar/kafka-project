package com.kafkalearning.consumer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * The consumer service's own producer — used exclusively by
 * DeadLetterPublishingRecoverer to republish failed records to the DLT.
 * This is NOT for general-purpose publishing from the consumer side.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> dltProducerFactory(KafkaProperties kafkaProperties){
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
    }

    @Bean
    public KafkaTemplate<String, String> dltKafkaTemplate(ProducerFactory<String, String> dltProducerFactory){
        return new KafkaTemplate<>(dltProducerFactory);
    }
}
