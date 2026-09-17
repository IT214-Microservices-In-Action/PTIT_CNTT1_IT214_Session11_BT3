package com.storex.loyalty.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(topics = "storex-order-events", groupId = "loyalty-group")
    public void consume(String message) {
        logger.info(String.format("$$$ -> Tieu thu message (CONG DIEM): %s", message));
        // Xu ly logic cong diem tai day
    }
}
