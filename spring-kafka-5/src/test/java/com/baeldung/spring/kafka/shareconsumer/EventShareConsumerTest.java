package com.baeldung.spring.kafka.shareconsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.baeldung.spring.kafka.shareconsumer.config.EventProducerConfig;
import com.baeldung.spring.kafka.shareconsumer.config.ShareConsumerConfig;
import com.baeldung.spring.kafka.shareconsumer.consumer.EventShareConsumer;
import com.baeldung.spring.kafka.shareconsumer.model.Event;

@EnableKafka
@SpringBootTest(
    classes = {
        ShareConsumerConfig.class,
        EventShareConsumer.class,
        EventProducerConfig.class
    }
)
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = "share-topic"
)
public class EventShareConsumerTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<String, Event> kafkaTemplate;

    @MockitoSpyBean
    private EventShareConsumer eventShareConsumer;

    @Test
    public void foo() throws InterruptedException, ExecutionException {
        kafkaTemplate.send("share-topic", new Event(1L, "manolis")).get();
        Thread.sleep(30000);
        verify(eventShareConsumer, times(1)).consume(any());
    }

}
