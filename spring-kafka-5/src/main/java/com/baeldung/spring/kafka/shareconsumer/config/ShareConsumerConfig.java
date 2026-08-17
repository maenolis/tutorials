package com.baeldung.spring.kafka.shareconsumer.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import com.baeldung.spring.kafka.shareconsumer.model.Event;

@Configuration
public class ShareConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ShareConsumerFactory<String, Event> shareConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.baeldung.spring.kafka.shareconsumer.model");
        ShareConsumerFactory<String, Event> factory = new DefaultShareConsumerFactory<>(props);
        factory.addListener(new ShareConsumerFactory.Listener<>() {
            @Override
            public void consumerAdded(String id, ShareConsumer<String, Event> consumer) {
                // Called when a new consumer is created
                System.out.println("Consumer added: " + id);
            }

            @Override
            public void consumerRemoved(String id, ShareConsumer<String, Event> consumer) {
                // Called when a consumer is closed
                System.out.println("Consumer removed: " + id);
            }
        });
        return factory;
    }

    @Bean
    public ShareKafkaListenerContainerFactory<String, Event>
    shareKafkaListenerContainerFactory(
        ShareConsumerFactory<String, Event> shareConsumerFactory) {
        ShareKafkaListenerContainerFactory<String, Event> factory = new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
        // factory.setConcurrency(5);
        return factory;
    }

}
