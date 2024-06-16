package com.examples.kafkastreams.config;

import com.examples.kafkastreams.dto.Payment;
import com.examples.kafkastreams.dto.Test;
import com.examples.kafkastreams.dto.ValidationResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
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

@Configuration
@EnableKafkaStreams
@EnableKafka
@Slf4j
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
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);
    props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Test.class);
    return new KafkaStreamsConfiguration(props);
  }


  @Bean
  public KStream<String, String> kStream(StreamsBuilder builder) {
    KStream<String, String> fileStream = builder.stream("file-input-topic",
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Payment> stringPaymentKStream = fileStream.flatMapValues(fileId -> {
      // Logic to read file and create payment objects
      List<Payment> payments = createPaymentsFromFile(fileId);
      return payments;
    });

    stringPaymentKStream.to("payment-validation-topic",
        Produced.with(Serdes.String(), new JsonSerde<>(Payment.class)));

    createPaymentValidationStream(builder);

    return fileStream;
  }

  private void createPaymentValidationStream(StreamsBuilder builder) {
    KStream<String, Payment> paymentValidationStream = builder.stream("payment-validation-topic",
        Consumed.with(Serdes.String(), new JsonSerde<>(Payment.class)));

    KStream<String, ValidationResult> validationResultStream = paymentValidationStream.mapValues(
        payment -> {
          boolean isValid = validate(payment);
          ValidationResult result = new ValidationResult();
          result.setFileId(payment.getFileId());
          result.setPaymentId(payment.getId());
          result.setValid(isValid);
          return result;
        });

    KTable<String, Long> validCountTable = validationResultStream.filter(
        (fileId, result) -> result.isValid()).groupBy((fileId, result) -> result.getFileId(),
        Grouped.with(Serdes.String(), new JsonSerde<>(ValidationResult.class))).count(
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("validPaymentCounts")
            .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

    // KTable to keep track of invalid counts
    KTable<String, Long> invalidCountTable = validationResultStream.filter(
        (fileId, result) -> !result.isValid()).groupBy((fileId, result) -> result.getFileId(),
        Grouped.with(Serdes.String(), new JsonSerde<>(ValidationResult.class))).count(
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("invalidPaymentCounts")
            .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

    invalidCountTable.toStream()
        .foreach((k, v) -> log.info("invalid count fileId {}, count {}", k, v));
    validCountTable.toStream().foreach((k, v) -> log.info("valid count fileId {}, count {}", k, v));


    /*ValueJoiner<Long, Long, TotalValidationCount> valueJoiner = TotalValidationCount::new;

    validCountTable.toStream().join(invalidCountTable.toStream(), valueJoiner, JoinWindows.of(
        Duration.ofSeconds(10))).foreach((fileId, totalValidationCount) -> {
      long totalPaymentCount = 10;
      if (totalValidationCount.getTotalValid() + totalValidationCount.getTotalInvalid() == totalPaymentCount) {
        log.info("all records validated");
      }
    });*/

  }

  public boolean validate(Payment payment) {
    log.info("validating payment {}", payment);
    return payment.getId() % 2 == 0;
  }

  private List<Payment> createPaymentsFromFile(String fileId) {
    List<Payment> paymentList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final Payment payment = new Payment();
      payment.setId(i);
      payment.setFileId(fileId);
      payment.setAmount(1 + 10);
      paymentList.add(payment);
    }
    return paymentList;
  }
}


