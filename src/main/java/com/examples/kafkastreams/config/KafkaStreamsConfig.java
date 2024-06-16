package com.examples.kafkastreams.config;

import com.examples.kafkastreams.dto.Test;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
@EnableKafka
public class KafkaStreamsConfig {
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    @Primary
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new JsonSerde<>(Test.class).getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);
        props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Test.class);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, Test> kStreamJson(StreamsBuilder builder) {
        KStream<String, Test> stream = builder.stream("streams-json-input", Consumed.with(Serdes.String(), new JsonSerde<>(Test.class)));

        KTable<String, Test> combinedDocuments = stream.map(new TestKeyValueMapper()).groupByKey().reduce(new TestReducer(), Materialized.<String, Test, KeyValueStore<Bytes, byte[]>>as("streams-json-store"));

        combinedDocuments.toStream().to("streams-json-output", Produced.with(Serdes.String(), new JsonSerde<>(Test.class)));

        return stream;
    }

    public static class TestKeyValueMapper implements KeyValueMapper<String, Test, KeyValue<String, Test>> {

        @Override
        public KeyValue<String, Test> apply(String key, Test value) {
            return new KeyValue<String, Test>(value.getKey(), value);
        }

    }

    public static class TestReducer implements Reducer<Test> {

        @Override
        public Test apply(Test value1, Test value2) {
            value1.getWords().addAll(value2.getWords());
            return value1;
        }

    }

}
