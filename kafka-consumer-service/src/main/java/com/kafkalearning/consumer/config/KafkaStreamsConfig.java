package com.kafkalearning.consumer.config;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME;

/**
 * Enables the Kafka Streams runtime as a second, independent processing
 * path alongside the existing {@code @KafkaListener}-based consumer
 * (see ADR-006). {@code @EnableKafkaStreams} activates Spring's
 * {@code StreamsBuilderFactoryBean} machinery, which
 * {@code HighValueOrderTopology} builds its topology on top of.
 *
 * <p><b>{@code application.id} is doing two jobs at once:</b> it names
 * the internal consumer group this Streams application uses to read
 * source topics, and it namespaces the local state directory Streams
 * creates on disk (relevant even though the current topology is
 * stateless — Streams still creates and locks this directory). It must
 * stay distinct from {@code spring.kafka.consumer.group-id} (used by
 * {@code OrderEventListener}) — these are two deliberately independent,
 * uncoordinated readers of {@code orders-events}; reusing the same group
 * identity between them would be a subtle, hard-to-diagnose bug.
 *
 * <p><b>Historical bug, worth knowing about:</b> this class originally
 * referenced the property key {@code spring.kafka.streams.application-id}
 * — a real, built-in Spring Boot property, but not the one actually
 * defined in this project's {@code application.yml}
 * ({@code app.kafka.streams.application-id}). Because it happened to be
 * a syntactically valid, "real-looking" property name, the mistake
 * wasn't obvious from reading the code — it only surfaced as a
 * {@code PlaceholderResolutionException} once a test tried to actually
 * load the full application context. Worth remembering: an unresolved
 * placeholder referencing a plausible-sounding property name can hide in
 * plain sight longer than an obviously wrong one would.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    /**
     * Builds the {@link KafkaStreamsConfiguration} bean that Spring's
     * {@code StreamsBuilderFactoryBean} consumes to construct the actual
     * {@code KafkaStreams} runtime instance. Bean name is fixed to
     * {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME}
     * — required for {@code @EnableKafkaStreams} to locate it.
     *
     * @param bootstrapServers shared Kafka bootstrap servers config,
     *                         same value used by the consumer/producer clients
     * @param applicationId    unique identity for this Streams application —
     *                         see class-level doc for why this must differ
     *                         from the {@code @KafkaListener} consumer group id
     * @return Streams runtime configuration
     */
    @Bean(name =  DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${app.kafka.streams.application-id}") String applicationId
    ){
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, applicationId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        return new KafkaStreamsConfiguration(props);
    }
}
