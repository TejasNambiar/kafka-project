package com.kafkalearning.producer.service;

import com.kafkalearning.producer.event.OrderEvent;

public interface OrderEventProducer {

    public void publishOrderEvent(OrderEvent event);
}