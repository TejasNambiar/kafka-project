package com.kafkalearning.consumer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.consumer.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Stateless Kafka Streams topology: {@code orders-events} →
 * filter (amount &gt; {@link #HIGH_VALUE_THRESHOLD}) → {@code high-value-orders}.
 *
 * <p>Deliberately narrow in scope (see ADR-006) — no aggregation, no
 * state store, no windowing. This is the first exposure to the Streams
 * DSL in this project; stateful operations are deferred to a later
 * phase so DSL fluency (source → transform → sink) is established
 * first, without also introducing RocksDB, changelog topics, and
 * fault-tolerant state recovery in the same step.
 *
 * <p><b>Known gap — not yet fixed:</b> the {@code catch} block in
 * {@link #highValueOrderStream} silently excludes any record that fails
 * to deserialize — it is <i>not</i> routed to a dead letter topic or
 * retried. This is a real asymmetry with {@code OrderEventListener},
 * which has full retry/backoff/DLT handling via {@code DefaultErrorHandler}.
 * Kafka Streams has its own, separate error-handling mechanism
 * ({@code deserialization.exception.handler}) that does not share
 * configuration with {@code DefaultErrorHandler} — wiring that up is
 * deferred, and this gap is called out explicitly so it is not mistaken
 * for parity with the {@code @KafkaListener} path's reliability guarantees.
 */
@Slf4j
@Configuration
public class HighValueOrderTopology {

    /**
     * Orders strictly greater than this amount are routed to
     * {@code high-value-orders}. An order at exactly this amount is
     * <b>not</b> considered high-value (see
     * {@code HighValueOrderTopologyTest#exactlyThresholdAmount_isNotRoutedAsHighValue}).
     */
    private static final double HIGH_VALUE_THRESHOLD = 1000.0;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    /**
     * Builds and registers the filter topology against the shared
     * {@link StreamsBuilder} provided by Spring's Streams autoconfiguration.
     *
     * @param streamsBuilder shared topology builder, managed by
     *                       {@code @EnableKafkaStreams}
     * @param sourceTopic    topic to read orders from ({@code orders-events})
     * @param sinkTopic      topic to write high-value matches to
     *                       ({@code high-value-orders})
     * @return the filtered stream, exposed as a bean primarily so its
     *         construction is visible in the application context and
     *         testable independently if needed; the topology is already
     *         fully wired via {@code .to(sinkTopic)} by the time this
     *         method returns
     */
    @Bean
    public KStream<String, String> highValueOrderStream(
            StreamsBuilder streamsBuilder,
            @Value("${app.kafka.topic.orders-events}") String sourceTopic,
            @Value("${app.kafka.topic.high-value-orders}") String sinkTopic
    ){
        KStream<String, String> source = streamsBuilder.stream(sourceTopic);

        KStream<String, String> highValueOnly = source.filter((key, value) -> {
            try {
                OrderEvent event = objectMapper.readValue(value, OrderEvent.class);
                boolean isHighValue = event.amount() > HIGH_VALUE_THRESHOLD;
                if (isHighValue) {
                    log.info("Routing high-value order to {}: orderId={} amount={}",
                            sinkTopic, event.orderId(), event.amount());
                }
                return isHighValue;
            } catch (Exception e) {
                // Streams' filter predicate has no checked-exception escape
                // hatch — a malformed record here is silently excluded
                // (not routed anywhere). This is a real gap: Streams'
                // deserialization.exception.handler is the proper fix,
                // out of scope for this phase per ADR-006. Logged loudly
                // so it isn't a silent, undiscoverable gap.
                log.error("Skipping unparseable record in high-value-order topology, key={}", key, e);
                return false;
            }
        });

        highValueOnly.to(sinkTopic);

        return highValueOnly;
    }
}
