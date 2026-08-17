package com.baeldung.spring.kafka.shareconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.baeldung.spring.kafka.shareconsumer.model.Event;

@Component
public class EventShareConsumer {

    @KafkaListener(
        topics = "stopic",
        groupId = "shgroup",
        containerFactory = "shareKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Event> record) {
        log.info("Doing some heavy work for key {} ...", record.key());
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("Finished heavy work for key {} ...", record.key());
    }

    private static final Logger log = LoggerFactory.getLogger(EventShareConsumer.class);

}
